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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection

title = 'Full NoiseModelling computation that output results for each UUEID separately'
description = 'Full NoiseModelling computation that output results for each UUEID separately'

inputs = [
        projectionName: [
                name: "Projection name",
                title: "Projection name",
                description: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ] ,
        uueid_pattern: [
                title: "UUEID pattern",
                name: "UUEID pattern",
                description: "UUEID pattern on roads to extract. eg. RD_FR_00_044% <p>A percent sign % - represents zero, one, or multiple characters</p>" +
                        "<p>A underscore sign _ - represents a single character</p>",
                type: String.class
        ],
        conf: [
                title: "Configuration identifier",
                name: "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type: Integer.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger(this.class)

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if(!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
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

        // Fetch configuration
        // CREATE TABLE cbs_uge_input.nm_conf (
        //	confid int4 NOT NULL,
        //	confreflorder int4 NULL,
        //	confmaxsrcdist int4 NULL,
        //	confmaxrefldist int4 NULL,
        //	confdistbuildingsreceivers int4 NULL,
        //	confthreadnumber int4 NULL,
        //	confdiffvertical bool NULL,
        //	confdiffhorizontal bool NULL,
        //	confskiplday bool NULL,
        //	confskiplevening bool NULL,
        //	confskiplnight bool NULL,
        //	confskiplden bool NULL,
        //	confexportsourceid bool NULL,
        //	wall_alpha float4 NULL,
        //	CONSTRAINT nm_conf_pkey PRIMARY KEY (confid)
        //);
        mainConfiguration = sql.firstRow("SELECT * FROM cbs_uge_input.nm_conf WHERE confid = ${input.conf}" as String)

        List<String> uueids = new ArrayList<>()
        sql.rows("SELECT DISTINCT uueid from cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid LIKE '${input.uueid_pattern}'" as String).each {
            row ->
                uueids.add(row.uueid as String)
        }
        ProgressVisitor stepsProgress = progress.subProcess(uueids.size()) // long running sub tasks

        if(uueids.isEmpty()) {
            throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}")
        }

        uueids.each {
            computeForUUEID(it, connection, pgConnection, stepsProgress, input, mainConfiguration)
        }

        // Return results
        return [result : "OK"]
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
    if(res == null) {
        throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
    }

    def extractionEnvelopeGeometry = ValueGeometry.getFromGeometry(res.geomenv as Geometry).string

    logger.info("SRID: {}", (res.geomenv as Geometry).getSRID())

    processBuildings(input, extractionEnvelopeGeometry, h2Connection, stepsProgress, mainConfiguration.wall_alpha as Double)

    processRoads(input, uueid, h2Connection, stepsProgress)
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
    def projectionName=input.projectionName
    def tableQuery = """SELECT b.geom3d as the_geom, b.bat_haut as height, b.idbat, p.pop_bat as pop
             FROM cbs_uge_input.c_batiment_s_${projectionName} b 
                INNER JOIN cbs_uge_input.c_population_${projectionName} p ON b.idbat = p.idbat  
             WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry)"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($tableQuery)" as String, tableName: "BUILDINGS"], stepsProgress)

    Sql sql = new Sql(h2Connection)
    sql.execute("""ALTER TABLE buildings ADD COLUMN g float DEFAULT $wallAlpha;""" as String)

    def erpsQuery = """SELECT idbat, b.erps_nature from cbs_uge_input.c_batimentsensible_${projectionName} b, cbs_uge_input.c_correspond_batiment_batimentsensible_${projectionName} a  WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry) AND a.iderps = b.iderps"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($erpsQuery)" as String, tableName: "BUILDINGS_ERPS", intermediateFileFormat : "json"], stepsProgress)

    def noiseBarrierQuery = """SELECT ST_Force3DZ(ST_CollectionHomogenize(geom)) as the_geom, hauteur as height FROM cbs_uge_input.n_routier_protection_acoustique_hexa AS nrpah WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry)"""

    ScriptUtilities.execScript(new Copy_PostGIS_To_H2GIS(), h2Connection, [tableToExport: "($noiseBarrierQuery)" as String, tableName: "BUILDINGS_BARRIERS"], stepsProgress)

    //remove constraint on geometry type
    // add barriers
    // remove Z when Z altitude is not good (will use HEIGHT field)
    def insertBarriersSql = """
        ALTER TABLE BUILDINGS ALTER COLUMN the_geom GEOMETRY;
        INSERT INTO BUILDINGS(the_geom, height) SELECT the_geom, height from BUILDINGS_BARRIERS;
        UPDATE BUILDINGS SET THE_GEOM = ST_Force2D(THE_GEOM) 
        WHERE ST_ZMIN(THE_GEOM) < -999 
        OR (ST_ZMIN(THE_GEOM) = 0 AND ST_ZMAX(THE_GEOM) = 0); 
        """

    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", insertBarriersSql, "outputFormat", "json"),
            new EmptyProgressVisitor())
}

//static Object execScript(Script script, Connection connection, Map inputs, ProgressVisitor progress) {
//    Logger logger = LoggerFactory.getLogger(this.class)
//    inputs = ScriptUtilities.fillDefaultValues(script.class, inputs);
//    logger.info("Run script: {} with inputs {}", script.getClass().getSimpleName(), inputs);
//    script.exec(connection, inputs, progress)
//}
