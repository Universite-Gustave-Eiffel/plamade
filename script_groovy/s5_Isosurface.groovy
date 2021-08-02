/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285 
 */

/* TODO
    - Check spatial index and srids
    - Add Metadatas
    - remove unnecessary lines (il y en a beaucoup)
    - Change Name of the file
    - Change descriptions
    - remove temporary tables
    - Export shp files
 */



package org.noise_planet.noisemodelling.wps.plamade;

import geoserver.GeoServer
import geoserver.catalog.Store
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.jdbc.BezierContouring
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import groovy.sql.Sql

import org.h2gis.utilities.JDBCUtilities

title = 'Create an isosurface from a NoiseModelling result table and its associated TRIANGLES table.'

description = 'Generate one isosurface table per road or rail infrastructure, both for LDEN and LNIGHT.'

inputs = [
        rail_or_road: [
                name       : 'Rail or Road',
                title      : 'Rail or Road',
                description: 'Rail (1) ou Road (2)',
                type: Integer.class
        ]
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]

static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

def exec(Connection connection, input) {

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // output string, the information given back to the user
    String resultString = "Le processus est terminé"

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Compute Isosurfaces')
    logger.info("inputs {}", input) // log inputs of the run

    Integer railRoad = input["rail_or_road"]

    // List of iso classes :  isoClasses
    // LDEN classes : 55-59, 60-64, 65-69, 70-74 et >75 dB
    def isoLevelsLDEN=[55.0d,60.0d,65.0d,70.0d,75.0d,200.0d]
    // LNIGHT classes : 50-54, 55-59, 60-64, 65-69 et >70 dB
    def isoLevelsLNIGHT=[50.0d,55.0d,60.0d,65.0d,70.0d,200.0d]


    // List of input tables : inputTable

    if (railRoad==1){
        source = "RAIL_SECTIONS"
        tableDEN = "LDEN_RAILWAY"
        tableNIGHT = "LNIGHT_RAILWAY"
    }
    else{
        source = "ROADS"
        tableDEN = "LDEN_ROADS"
        tableNIGHT = "LNIGHT_ROADS"
    }

    String ldenInput = "RECEIVERS_DELAUNAY_DEN"
    String lnightInput = "RECEIVERS_DELAUNAY_NIGHT"

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)

    sql.execute("DROP TABLE IF EXISTS LDEN_GEOM, LNIGHT_GEOM, RECEIVERS_DEN, RECEIVERS_NIGHT")
    logger.info("Add infrastructure UUID into results table")
    sql.execute(String.format("CREATE TABLE LDEN_GEOM AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID , b.pk FROM " + tableDEN + " a, "+source+" b WHERE a.idsource=b.pk;"))
    sql.execute(String.format("CREATE TABLE LNIGHT_GEOM AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID , b.pk FROM " + tableNIGHT + " a, "+source+" b WHERE a.idsource=b.pk;"))

    // Ajout d'index pour accélerer le GROUP BY à venir.
    // Sur la zone de test, l'ajout des index fais perdre 5s. A voir sur un département entier s'ils font gagner du temps
    logger.info("Create indexes into results table")
    sql.execute("CREATE INDEX ON LDEN_GEOM(idreceiver)")
    sql.execute("CREATE INDEX ON LDEN_GEOM(UUEID)")
    sql.execute("CREATE INDEX ON LNIGHT_GEOM(idreceiver)")
    sql.execute("CREATE INDEX ON LNIGHT_GEOM(UUEID)")

    logger.info("Sum noise levels by infrastructure identifier")
    sql.execute("CREATE TABLE RECEIVERS_DEN AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LDEN_GEOM GROUP BY idreceiver, UUEID;")
    sql.execute("CREATE TABLE RECEIVERS_NIGHT AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LNIGHT_GEOM GROUP BY idreceiver, UUEID;")

    sql.execute("DROP TABLE IF EXISTS LDEN_GEOM, LNIGHT_GEOM")

    // -------------------
    // Initialisation des tables dans lesquelles on stockera les surfaces par tranche d'iso, par type d'infra et d'indice

    String nuts = sql.firstRow("SELECT NUTS FROM METADATA").nuts
    String cbsARoadLden = "CBS_A_ROUT_LDEN_"+nuts
    String cbsARoadLnight = "CBS_A_ROUT_LNIGHT_"+nuts
    String cbsAFerLden = "CBS_A_FER_LDEN_"+nuts
    String cbsAFerLnight = "CBS_A_FER_LNIGHT_"+nuts

    // Tables are created according to the input parameter "rail" or "road"
    if (railRoad==1){
        sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLden)
        sql.execute("CREATE TABLE "+ cbsAFerLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)")
        sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLnight)
        sql.execute("CREATE TABLE "+ cbsAFerLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)")
    }
    else{
        sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLden)
        sql.execute("CREATE TABLE "+ cbsARoadLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)")
        sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLnight)
        sql.execute("CREATE TABLE "+ cbsARoadLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)")
    }



    logger.info("Process each rail or road infrastructures")
    // Process each rail or road infrastructures
    sql.eachRow("SELECT DISTINCT UUEID FROM RECEIVERS_DEN") { row ->
        String uueid = row[0] as String
            
        String ldenOutput = uueid + "_CONTOURING_LDEN"
        String lnightOutput = uueid + "_CONTOURING_LNIGHT"
        
        sql.execute(String.format("DROP TABLE IF EXISTS "+ ldenOutput +", "+ lnightOutput +", RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN"))

        sql.execute(String.format("CREATE TABLE RECEIVERS_DELAUNAY_NIGHT (THE_GEOM geometry, UUEID varchar, PK Integer NOT NULL PRIMARY KEY, LAEQ float) AS SELECT b.THE_GEOM, an.UUEID, b.PK_1, an.laeqpa_sum FROM RECEIVERS_NIGHT an, receivers b WHERE an.idreceiver=b.PK AND RCV_TYPE=2 AND an.UUEID = '"+uueid+"' ;"))
        sql.execute(String.format("CREATE TABLE RECEIVERS_DELAUNAY_DEN (THE_GEOM geometry, UUEID varchar, PK Integer NOT NULL PRIMARY KEY, LAEQ float) AS SELECT b.THE_GEOM , aden.UUEID, b.PK_1, aden.laeqpa_sum FROM  RECEIVERS_DEN aden, receivers b WHERE aden.idreceiver=b.PK AND RCV_TYPE=2 AND aden.UUEID = '"+uueid+"' ;"))

        sql.execute("DROP TABLE IF EXISTS TRIANGLES ")
        sql.execute("CREATE TABLE TRIANGLES AS SELECT a.* FROM TRIANGLES_DELAUNAY a, RECEIVERS_DELAUNAY_NIGHT b, RECEIVERS_DELAUNAY_NIGHT c, RECEIVERS_DELAUNAY_NIGHT d WHERE a.PK_1=b.PK AND a.PK_2=c.PK AND a.PK_3=d.PK")

        // Produce isocontours for LNIGHT
        generateIsoSurfaces(lnightInput, isoLevelsLNIGHT, lnightOutput, connection, uueid, 'LNIGHT', input)

        sql.execute("DROP TABLE IF EXISTS TRIANGLES ")
        sql.execute("CREATE TABLE TRIANGLES AS SELECT a.* FROM TRIANGLES_DELAUNAY a, RECEIVERS_DELAUNAY_DEN b, RECEIVERS_DELAUNAY_DEN c, RECEIVERS_DELAUNAY_DEN d WHERE a.PK_1=b.PK AND a.PK_2=c.PK AND a.PK_3=d.PK")

        // Produce isocontours for LDEN
        generateIsoSurfaces(ldenInput, isoLevelsLDEN, ldenOutput, connection, uueid, 'LDEN', input)

        sql.execute("DROP TABLE IF EXISTS RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN, TRIANGLES1, TRIANGLES2, TRIANGLES3, TRIANGLES")
    }

    if (railRoad==1) {
        sql.execute("CALL SHPWrite('data_dir/data/shapefiles/" + cbsARoadLden + ".shp', '" + cbsARoadLden.toUpperCase(Locale.ROOT) + "');")
        sql.execute("CALL SHPWrite('data_dir/data/shapefiles/" + cbsARoadLnight + ".shp', '" + cbsARoadLnight.toUpperCase(Locale.ROOT) + "');")
    } else {
        sql.execute("CALL SHPWrite('data_dir/data/shapefiles/" + cbsAFerLden + ".shp', '" + cbsAFerLden.toUpperCase(Locale.ROOT) + "');")
        sql.execute("CALL SHPWrite('data_dir/data/shapefiles/" + cbsAFerLnight + ".shp', '" + cbsAFerLnight.toUpperCase(Locale.ROOT) + "');")
    }

    // print to WPS Builder
    return resultString
}

/**
 * Generate isosurfaces for LDEN and LNIGHT tables
 * @param inputTable
 * @param isoClasses
 * @param outputTable
 * @param connection
 * @param uueid
 * @param period
 * @param input
 * @return
 */
def generateIsoSurfaces(def inputTable, def isoClasses, def outputTable, def connection, def uueid, def period, def input) {

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    if(!JDBCUtilities.tableExists(connection, inputTable)) {
        logger.info "La table $inputTable n'est pas présente"
        return
        }
        int srid = SFSUtilities.getSRID(connection, TableLocation.parse(inputTable))

        Integer railRoad = input["rail_or_road"]

        BezierContouring bezierContouring = new BezierContouring(isoClasses, srid)

        bezierContouring.setPointTable(inputTable)
        double coefficient = 1
        bezierContouring.setSmooth(true)
        bezierContouring.setSmoothCoefficient(coefficient)

        bezierContouring.createTable(connection)

        Sql sql = new Sql(connection)

        String nuts = sql.firstRow("SELECT NUTS FROM METADATA").nuts
        String cbsARoadLden = "CBS_A_ROUT_LDEN_"+nuts
        String cbsARoadLnight = "CBS_A_ROUT_LNIGHT_"+nuts
        String cbsAFerLden = "CBS_A_FER_LDEN_"+nuts
        String cbsAFerLnight = "CBS_A_FER_LNIGHT_"+nuts

        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE CONTOURING_NOISE_MAP SET THE_GEOM=ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))")
        
        // Generate temporary table to store ISO areas
        sql.execute("DROP TABLE IF EXISTS ISO_AREA")
        sql.execute("CREATE TABLE ISO_AREA (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float) AS SELECT ST_UNION(ST_Accum(ST_Buffer(the_geom,0.001))), null, '"+uueid+"', '"+period+"', ISOLABEL, SUM(ST_AREA(the_geom)) FROM CONTOURING_NOISE_MAP GROUP BY ISOLABEL")
        // Update noise classes for LDEN
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '< 55' THEN  'LdenSmallerThan55' WHEN NOISELEVEL = '55-60' THEN  'Lden5559' WHEN NOISELEVEL = '60-65' THEN  'Lden6064' WHEN NOISELEVEL = '65-70' THEN  'Lden6569' WHEN NOISELEVEL = '70-75' THEN  'Lden7074' WHEN NOISELEVEL = '> 75' THEN  'LdenGreaterThan75' END) WHERE PERIOD='LDEN';")
        // Update noise classes for LNIGHT
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '< 50' THEN  'LnightSmallerThan50' WHEN NOISELEVEL = '50-55' THEN  'Lnight5054'  WHEN NOISELEVEL = '55-60' THEN  'Lnight5559' WHEN NOISELEVEL = '60-65' THEN  'Lnight6064' WHEN NOISELEVEL = '65-70' THEN  'Lnight6570'  WHEN NOISELEVEL = '> 70' THEN  'LnightGreaterThan70' END) WHERE PERIOD='LNIGHT';")
        // Generate the PK
        sql.execute("UPDATE ISO_AREA SET pk = CONCAT(uueid, '_',noiselevel)")
        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE ISO_AREA SET THE_GEOM=ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))")

        // Insert iso areas into common table, according to rail or road input parameter
        if (railRoad==1){
            sql.execute("INSERT INTO "+cbsAFerLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE PERIOD='LDEN'")
            sql.execute("INSERT INTO "+cbsAFerLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE PERIOD='LNIGHT'")
        }
        else{
            sql.execute("INSERT INTO "+cbsARoadLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE PERIOD='LDEN'")
            sql.execute("INSERT INTO "+cbsARoadLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE PERIOD='LNIGHT'")
        }


        sql.execute("DROP TABLE IF EXISTS " + outputTable+", ISO_AREA")
        // Rename CONTOURING_NOISE_MAP with the infrastructure UUEID
        sql.execute("ALTER TABLE CONTOURING_NOISE_MAP RENAME TO " + outputTable)

        resultString = "Table " + outputTable + " created"

        logger.info('End : Compute Isosurfaces')
        logger.info(resultString)
}

def run(input) {
    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}


