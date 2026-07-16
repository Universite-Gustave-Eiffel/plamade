package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import org.h2.value.ValueGeometry
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.spatial.convert.ST_AsWKT
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Geometry
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query
import org.noise_planet.noisemodelling.webserver.utilities.Logging
import org.noise_planet.noisemodelling.webserver.utilities.StringUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection

title = 'Full NoiseModelling computation that output results for each UUEID separately'
description = 'Full NoiseModelling computation that output results for each UUEID separately'

inputs = [
        projectionName: [
                name         : "Projection name",
                title        : "Projection name",
                description  : "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type         : String.class
        ],
        uueid_pattern : [
                title      : "UUEID pattern",
                name       : "UUEID pattern",
                description: "UUEID pattern on roads to extract. eg. RD_FR_00_044% <p>A percent sign % - represents zero, one, or multiple characters</p>" +
                        "<p>A underscore sign _ - represents a single character</p>",
                type       : String.class
        ],
        conf          : [
                title      : "Configuration identifier",
                name       : "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type       : Integer.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger(this.class)

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if (!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
        throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
    }

    def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

    def mainConfiguration = [:]
    Geometry extractionEnvelopeGeometry = null
    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            postgisConfig['user_name'] as String,
            postgisConfig['password'] as String,
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        pgConnection.setAutoCommit(true)
        Sql sql = new Sql(pgConnection)

        mainConfiguration = sql.firstRow("SELECT * FROM cbs_uge_input.nm_conf WHERE confid = ${input.conf}" as String)

        List<String> uueids = new ArrayList<>()
        sql.rows("SELECT DISTINCT uueid from cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid LIKE '${input.uueid_pattern}'" as String).each {
            row ->
                uueids.add(row.uueid as String)
        }
        ProgressVisitor stepsProgress = progress.subProcess(uueids.size()) // long running sub tasks

        if (uueids.isEmpty()) {
            throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
        }

        uueids.each {
            computeForUUEID(it, connection, pgConnection, stepsProgress, input, mainConfiguration)
        }

        // Return results
        return [result: "OK"]
    }


}

def computeForUUEID(String uueid, Connection h2Connection, Connection pgConnection, ProgressVisitor progress, Map input, Map mainConfiguration) {
    ProgressVisitor stepsProgress = progress.subProcess(3) // long running sub tasks
    def pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Computing for UUEID: $uueid")

    // Compute envelope of the simulation
    def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(st_convexhull(st_collect(the_geom)), ${mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE uueid LIKE '$uueid';""" as String)
    if (res == null) {
        throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
    }

    def extractionEnvelopeGeometry = ValueGeometry.getFromGeometry(res.geomenv as Geometry).string

    logger.info("SRID: {}", (res.geomenv as Geometry).getSRID())

    processBuildings(input, extractionEnvelopeGeometry, h2Connection, stepsProgress, mainConfiguration.wall_alpha as Double)

    processRoads(input, uueid, h2Connection, stepsProgress)

    processLandCover(input, extractionEnvelopeGeometry, h2Connection, stepsProgress)

    fetchAtmosphericPeriodFromStations(input, uueid, h2Connection, stepsProgress)
}

def processLandCover(Map input, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch land cover..")
    def landCoverQuery = """SELECT geom as the_geom, idnatsol as pk, natsol_lib as clc_lib, natsol_cno as g 
        FROM cbs_uge_input.c_naturesol_${input.projectionName} 
        WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry) AND NATSOL_CNO > 0"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($landCoverQuery)" as String, tableName: "LANDCOVER"], stepsProgress)
}

/**
 * Fetches atmospheric periods from nearby stations for a given UUEID.
 * @param input The input map containing configuration parameters.
 * @param uueid The road UUEID for which to fetch atmospheric periods.
 * @param h2Connection The H2 database connection.
 * @param stepsProgress The progress visitor for tracking execution progress.
 */
def fetchAtmosphericPeriodFromStations(Map input, String uueid, Connection h2Connection, ProgressVisitor stepsProgress) {
    // tablePeriodAtmosphericSettings — Atmospheric settings table name for each time period
    //
    //    Name of the Atmospheric settings table The table must contain the following columns:
    //
    //        PERIOD : time period (VARCHAR PRIMARY KEY)
    //        WINDROSE : probability of occurrences of favourable propagation conditions (ARRAY(16))
    //        TEMPERATURE : Temperature in celsius (FLOAT)
    //        PRESSURE : air pressure in pascal (FLOAT)
    //        HUMIDITY : air humidity in percentage (FLOAT)
    //        GDISC : choose between accept G discontinuity or not (BOOLEAN) default true
    //        PRIME2520 : choose to use prime values to compute eq. 2.5.20 (BOOLEAN) default false
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch nearest atmospheric points periods for UUEID: {}", uueid)

    def atmosphericQuery = """
        SELECT a.*, ST_Distance(a.the_geom, b.geom) as distance_station
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName} b, cbs_uge_input.nm_stations_${input.projectionName} a
        WHERE UUEID = '$uueid'
        ORDER BY a.the_geom <-> b.geom LIMIT 1
    """
    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($atmosphericQuery)" as String, tableName: "ATMOSPHERIC", intermediateFileFormat: "fgb"], stepsProgress)

    def sql = new Sql(h2Connection)
    def distanceStation = sql.firstRow("SELECT distance_station FROM ATMOSPHERIC")
    logger.info("Distance of ${uueid} to nearest station: ${Math.round(distanceStation.distance_station as double)} m")

    def generateAtmosphericSettingsQuery = """
        DROP TABLE IF EXISTS ATMOSPHERIC_SETTINGS;
        CREATE TABLE ATMOSPHERIC_SETTINGS(
            PERIOD VARCHAR,
            WINDROSE real ARRAY[16],
            TEMPERATURE NUMERIC,
            PRESSURE NUMERIC,
            HUMIDITY NUMERIC,
            GDISC BOOLEAN DEFAULT TRUE,
            PRIME2520 BOOLEAN DEFAULT FALSE
        );
        -- Insert time periods
        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'D', 
            ARRAY[
                /* 0.0°   */ pfav_6_18_0,
                /* 22.5°  */ (0.875 * pfav_6_18_20)  + (0.125 * pfav_6_18_40),
                /* 45.0°  */ (0.75  * pfav_6_18_40)  + (0.25  * pfav_6_18_60),
                /* 67.5°  */ (0.625 * pfav_6_18_60)  + (0.375 * pfav_6_18_80),
                /* 90.0°  */ (0.5   * pfav_6_18_80)  + (0.5   * pfav_6_18_100),
                /* 112.5° */ (0.375 * pfav_6_18_100) + (0.625 * pfav_6_18_120),
                /* 135.0° */ (0.25  * pfav_6_18_120) + (0.75  * pfav_6_18_140),
                /* 157.5° */ (0.125 * pfav_6_18_140) + (0.875 * pfav_6_18_160),
                /* 180.0° */ pfav_6_18_180,
                /* 202.5° */ (0.875 * pfav_6_18_200) + (0.125 * pfav_6_18_220),
                /* 225.0° */ (0.75  * pfav_6_18_220) + (0.25  * pfav_6_18_240),
                /* 247.5° */ (0.625 * pfav_6_18_240) + (0.375 * pfav_6_18_260),
                /* 270.0° */ (0.5   * pfav_6_18_260) + (0.5   * pfav_6_18_280),
                /* 292.5° */ (0.375 * pfav_6_18_280) + (0.625 * pfav_6_18_300),
                /* 315.0° */ (0.25  * pfav_6_18_300) + (0.75  * pfav_6_18_320),
                /* 337.5° */ (0.125 * pfav_6_18_320) + (0.875 * pfav_6_18_340)
            ], 
            temp_6_18, 
            101325, 
            hygro_6_18 * 100 
        FROM ATMOSPHERIC; 
        
        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'E', 
            ARRAY[
                /* 0.0°   */ pfav_18_22_0,
                /* 22.5°  */ (0.875 * pfav_18_22_20)  + (0.125 * pfav_18_22_40),
                /* 45.0°  */ (0.75  * pfav_18_22_40)  + (0.25  * pfav_18_22_60),
                /* 67.5°  */ (0.625 * pfav_18_22_60)  + (0.375 * pfav_18_22_80),
                /* 90.0°  */ (0.5   * pfav_18_22_80)  + (0.5   * pfav_18_22_100),
                /* 112.5° */ (0.375 * pfav_18_22_100) + (0.625 * pfav_18_22_120),
                /* 135.0° */ (0.25  * pfav_18_22_120) + (0.75  * pfav_18_22_140),
                /* 157.5° */ (0.125 * pfav_18_22_140) + (0.875 * pfav_18_22_160),
                /* 180.0° */ pfav_18_22_180,
                /* 202.5° */ (0.875 * pfav_18_22_200) + (0.125 * pfav_18_22_220),
                /* 225.0° */ (0.75  * pfav_18_22_220) + (0.25  * pfav_18_22_240),
                /* 247.5° */ (0.625 * pfav_18_22_240) + (0.375 * pfav_18_22_260),
                /* 270.0° */ (0.5   * pfav_18_22_260) + (0.5   * pfav_18_22_280),
                /* 292.5° */ (0.375 * pfav_18_22_280) + (0.625 * pfav_18_22_300),
                /* 315.0° */ (0.25  * pfav_18_22_300) + (0.75  * pfav_18_22_320),
                /* 337.5° */ (0.125 * pfav_18_22_320) + (0.875 * pfav_18_22_340)
            ], 
            temp_18_22, 
            101325, 
            hygro_18_22 * 100 
        FROM ATMOSPHERIC;    

        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'N', 
            ARRAY[
                /* 0.0°   */ pfav_22_6_0,
                /* 22.5°  */ (0.875 * pfav_22_6_20)  + (0.125 * pfav_22_6_40),
                /* 45.0°  */ (0.75  * pfav_22_6_40)  + (0.25  * pfav_22_6_60),
                /* 67.5°  */ (0.625 * pfav_22_6_60)  + (0.375 * pfav_22_6_80),
                /* 90.0°  */ (0.5   * pfav_22_6_80)  + (0.5   * pfav_22_6_100),
                /* 112.5° */ (0.375 * pfav_22_6_100) + (0.625 * pfav_22_6_120),
                /* 135.0° */ (0.25  * pfav_22_6_120) + (0.75  * pfav_22_6_140),
                /* 157.5° */ (0.125 * pfav_22_6_140) + (0.875 * pfav_22_6_160),
                /* 180.0° */ pfav_22_6_180,
                /* 202.5° */ (0.875 * pfav_22_6_200) + (0.125 * pfav_22_6_220),
                /* 225.0° */ (0.75  * pfav_22_6_220) + (0.25  * pfav_22_6_240),
                /* 247.5° */ (0.625 * pfav_22_6_240) + (0.375 * pfav_22_6_260),
                /* 270.0° */ (0.5   * pfav_22_6_260) + (0.5   * pfav_22_6_280),
                /* 292.5° */ (0.375 * pfav_22_6_280) + (0.625 * pfav_22_6_300),
                /* 315.0° */ (0.25  * pfav_22_6_300) + (0.75  * pfav_22_6_320),
                /* 337.5° */ (0.125 * pfav_22_6_320) + (0.875 * pfav_22_6_340)
            ], 
            temp_22_6, 
            101325, 
            hygro_22_6 * 100 
        FROM ATMOSPHERIC;

        DROP TABLE ATMOSPHERIC;
    """

    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", generateAtmosphericSettingsQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

    logger.info( Logging.formatSqlQueryResult(sql, "SELECT PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY FROM ATMOSPHERIC_SETTINGS", 120))

}

def processRoads(Map input, String uueid, Connection h2Connection, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch roads..")
    def roadsQuery = """SELECT * FROM cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid LIKE '$uueid'"""
    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($roadsQuery)" as String, tableName: "LW_ROADS"], stepsProgress)
}

def processBuildings(Map input, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress, double wallAlpha) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch buildings..")
    def projectionName = input.projectionName
    def tableQuery = """SELECT b.geom3d as the_geom, b.bat_haut as height, b.idbat, p.pop_bat as pop, b.bat_idtopo
             FROM cbs_uge_input.c_batiment_s_${projectionName} b 
                INNER JOIN cbs_uge_input.c_population_${projectionName} p ON b.idbat = p.idbat  
             WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry)"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($tableQuery)" as String, tableName: "BUILDINGS"], stepsProgress)

    Sql sql = new Sql(h2Connection)
    sql.execute("""ALTER TABLE buildings ADD COLUMN g float DEFAULT $wallAlpha;""" as String)

    def erpsQuery = """SELECT idbat, b.erps_nature from cbs_uge_input.c_batimentsensible_${projectionName} b, cbs_uge_input.c_correspond_batiment_batimentsensible_${projectionName} a  WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry) AND a.iderps = b.iderps"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($erpsQuery)" as String, tableName: "BUILDINGS_ERPS", intermediateFileFormat: "json"], stepsProgress)

    def noiseBarrierQuery = """SELECT ST_Force3DZ(ST_CollectionHomogenize(geom)) as the_geom, hauteur as height, propriete, materiau1, idprotacou FROM cbs_uge_input.n_routier_protection_acoustique_hexa AS nrpah WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry)"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($noiseBarrierQuery)" as String, tableName: "BUILDINGS_BARRIERS"], stepsProgress)

    //remove constraint on geometry type
    // add barriers with densification 1 meter (will follow the dem)
    // remove Z when Z altitude is not good (will use HEIGHT field)
    def insertBarriersSql = """

        ALTER TABLE BUILDINGS_BARRIERS ADD COLUMN G float DEFAULT 0;
    
        UPDATE BUILDINGS_BARRIERS SET G = 0.7 WHERE propriete = '01';
        UPDATE BUILDINGS_BARRIERS SET G = 0.7 WHERE (propriete = '00' or propriete = '99') AND (materiau1 = '01' or materiau1 = '04' or materiau1 = '06');
        
        DROP TABLE IF EXISTS BUILDINGS_BARRIER_EXPLODED;
        CREATE TABLE BUILDINGS_BARRIER_EXPLODED AS SELECT * FROM ST_EXPLODE('(SELECT ST_ToMultiSegments(st_densify(the_geom, 1)) the_geom, height, G, idprotacou FROM BUILDINGS_BARRIERS)');
        
        -- Use generic type in order to mix polygon and linestring
        ALTER TABLE BUILDINGS ALTER COLUMN the_geom GEOMETRY;
        -- Add origin column, road is acoustic protection along roads
        ALTER TABLE BUILDINGS ADD COLUMN origin varchar DEFAULT 'building';
        INSERT INTO BUILDINGS(the_geom, height, G, origin, pop, idbat, bat_idtopo) SELECT the_geom, height, G, 'road', 0, '', idprotacou from BUILDINGS_BARRIER_EXPLODED;
        UPDATE BUILDINGS SET THE_GEOM = ST_Force2D(THE_GEOM) 
        WHERE ST_ZMIN(THE_GEOM) < -999 
        OR (ST_ZMIN(THE_GEOM) = 0 AND ST_ZMAX(THE_GEOM) = 0);
        
        DROP TABLE BUILDINGS_BARRIERS, BUILDINGS_BARRIER_EXPLODED;
        """

    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", insertBarriersSql, "outputFormat", "json"),
            new EmptyProgressVisitor())

}
