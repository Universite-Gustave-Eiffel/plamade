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
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285 
 */

/* TODO
    - Check spatial index and srids
    - Add Metadatas
    - remove unnecessary lines (il y en a beaucoup)
    - Do it for every ISOCOntours --> done
    - Change Name of the file
    - Calculate EPRS, etc. 
    - add some logs for the user --> done
    - Change descriptions --> done
    - remove temporary tables --> done
    - Export csv files
 */
package org.noise_planet.noisemodelling.wps.plamade;


import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

title = 'Buildings Grid'
description = 'Generates receivers placed 2 meters from building facades at specified height.' +
        '</br> </br> <b> The output table is called : RECEIVERS </b>'

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

def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info(String.format("You have chosen to calculate indicatorts with configuration number : %d [Rail(1) or Road(2)]", input.rail_or_road))
    
    def rail_or_road = input["rail_or_road"]
    def tableNIGHT
    def tableDEN
    def source
    def outputDEN
    def outputNIGHT

    if (rail_or_road==1){
        tableDEN = "LDEN_RAILWAY"
        tableNIGHT = "LNIGHT_RAILWAY"
        source = "RAIL_SECTIONS"
        outputDEN = "RAILWAY_POP_DEN"
        outputNIGHT = "RAILWAY_POP_NIGHT"

    }
    else{
        tableDEN = "LDEN_ROADS"
        tableNIGHT = "LNIGHT_ROADS"
        source = "ROADS"
        outputDEN = "ROADS_POP_DEN"
        outputNIGHT = "ROADS_POP_NIGHT"

    }
    
    def srid = SFSUtilities.getSRID(connection, TableLocation.parse(source))


    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)

    logger.info("Count buildings, schools and hospitals")

    sql.execute("DROP TABLE IF EXISTS BUILDING_SCHOOL, BUILDING_HOSPITAL, BUILDING_COUNT_1, BUILDING_COUNT;")
    sql.execute("CREATE TABLE BUILDING_SCHOOL (BUILD_PK varchar, SCHOOL Integer) AS SELECT PK, 1 FROM BUILDINGS_SCREENS WHERE ERPS_NATUR='Enseignement';")
    sql.execute("CREATE TABLE BUILDING_HOSPITAL (BUILD_PK varchar, HOSPITAL Integer) AS SELECT PK, 1 FROM BUILDINGS_SCREENS WHERE ERPS_NATUR='Sante';")
    sql.execute("CREATE TABLE BUILDING_COUNT_1 (BUILD_PK varchar, BUILDING integer, SCHOOL integer) AS SELECT a.PK, 1, b.SCHOOL FROM BUILDINGS_SCREENS a LEFT JOIN BUILDING_SCHOOL b ON a.PK=b.BUILD_PK;")
    sql.execute("CREATE TABLE BUILDING_COUNT (BUILD_PK varchar PRIMARY KEY, BUILDING integer, SCHOOL integer, HOSPITAL integer) AS SELECT a.*, b.HOSPITAL FROM BUILDING_COUNT_1 a LEFT JOIN BUILDING_HOSPITAL b ON a.BUILD_PK=b.BUILD_PK;")
    sql.execute("DROP TABLE IF EXISTS BUILDING_SCHOOL, BUILDING_HOSPITAL, BUILDING_COUNT_1;")


    logger.info("Start computing working tables");

    sql.execute("DROP TABLE IF EXISTS LDEN_GEOM_INFRA, LNIGHT_GEOM_INFRA, RECEIVERS_SUM_LAEQPA_DEN, RECEIVERS_SUM_LAEQPA_NIGHT, RECEIVERS_BUILD_NIGHT, RECEIVERS_BUILD_DEN, RECEIVERS_BUILD_POP_NIGHT, RECEIVERS_BUILD_POP_DEN, BUILD_MAX_NIGHT, BUILD_MAX_DEN;")

    sql.execute("CREATE TABLE LDEN_GEOM_INFRA AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID, b.pk FROM " + tableDEN + " a, "+source+" b WHERE a.idsource=b.pk;")
    sql.execute("CREATE TABLE LNIGHT_GEOM_INFRA AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID, b.pk FROM " + tableNIGHT + " a, "+source+" b WHERE a.idsource=b.pk;")
    
    sql.execute("CREATE TABLE RECEIVERS_SUM_LAEQPA_DEN AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LDEN_GEOM_INFRA GROUP BY idreceiver, UUEID  ;")
    sql.execute("CREATE TABLE RECEIVERS_SUM_LAEQPA_NIGHT AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LNIGHT_GEOM_INFRA GROUP BY idreceiver, UUEID  ;")

    sql.execute("CREATE TABLE RECEIVERS_BUILD_DEN AS SELECT aden.UUEID as UUEID, b.PK_1 PK, aden.laeqpa_sum as lden FROM RECEIVERS_SUM_LAEQPA_DEN aden, receivers b WHERE aden.idreceiver=b.PK AND RCV_TYPE=1;")
    sql.execute("CREATE TABLE RECEIVERS_BUILD_NIGHT AS SELECT an.UUEID as UUEID, b.PK_1 PK, an.laeqpa_sum as lnight FROM RECEIVERS_SUM_LAEQPA_NIGHT an, receivers b WHERE an.idreceiver=b.PK AND RCV_TYPE=1 ;")

    sql.execute("CREATE TABLE RECEIVERS_BUILD_POP_DEN AS SELECT aden.UUEID as UUEID, aden.PK, b.BUILD_PK as BUILD_PK, b.pop as POP, aden.lden as lden FROM RECEIVERS_BUILD_DEN aden, receivers_BUILDING b WHERE aden.PK=b.PK ;")
    sql.execute("CREATE TABLE RECEIVERS_BUILD_POP_NIGHT AS SELECT an.UUEID as UUEID, an.PK, b.BUILD_PK as BUILD_PK, b.pop as POP, an.lnight as lnight FROM RECEIVERS_BUILD_NIGHT an, receivers_BUILDING b WHERE an.PK=b.PK ;")



    sql.execute("CREATE TABLE BUILD_MAX_DEN AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lden) as lden FROM RECEIVERS_BUILD_POP_DEN a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")
    sql.execute("CREATE TABLE BUILD_MAX_NIGHT AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lnight) as lnight FROM RECEIVERS_BUILD_POP_NIGHT a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")
    //sql.execute("CREATE TABLE BUILD_MAX_NIGHT AS SELECT UUEID, ROUND(SUM(POP),1) as POP, BUILD_PK, max(lnight) as lnight FROM RECEIVERS_BUILD_POP_NIGHT GROUP BY UUEID, BUILD_PK;")

    sql.execute("DROP TABLE IF EXISTS "+outputDEN+","+outputNIGHT+";")

    //
    // For NIGHT 

    // LNIGHT classes : 50-54, 55-59, 60-64, 65-69 et >70 dB

    logger.info("Start computing LNIGHT tables")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5;")

    // rajouter SOMME de BUILDING, SCHOOL, HOSPITAL

    sql.execute("CREATE TABLE ROADS_POP_NIGHT1 AS SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 0.0 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight<50 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT2 AS SELECT * FROM ROADS_POP_NIGHT1 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 52.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight>50 AND lnight<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT3 AS SELECT * FROM ROADS_POP_NIGHT2 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 57.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight>55 and lnight<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT4 AS SELECT * FROM ROADS_POP_NIGHT3 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 62.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight>60 and lnight<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT5 AS SELECT * FROM ROADS_POP_NIGHT4 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 67.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight>65 and lnight<70 GROUP BY UUEID;")

    // final table
    sql.execute("CREATE TABLE "+outputNIGHT+" AS SELECT * FROM ROADS_POP_NIGHT5 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 72.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_NIGHT WHERE lnight>70 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputNIGHT+" ADD HSD FLOAT;")
    sql.execute("UPDATE "+outputNIGHT+" SET HSD = ROUND(sum_pop*(19.4312-0.9336*range+0.0126*range*range)/100,1) ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5;")

    // 
    // For DAY 

    // LDEN classes : 55-59, 60-64, 65-69, 70-74 et >75 dB

    logger.info("Start computing LDEN tables")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5;")

    sql.execute("CREATE TABLE ROADS_POP_DEN1 AS SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 0.0 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_DEN WHERE lden<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN2 AS SELECT * FROM ROADS_POP_DEN1 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 57.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_DEN WHERE lden>55 AND lden<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN3 AS SELECT * FROM ROADS_POP_DEN2 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 62.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_DEN WHERE lden>60 and lden<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN4 AS SELECT * FROM ROADS_POP_DEN3 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 67.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_DEN WHERE lden>65 and lden<70 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN5 AS SELECT * FROM ROADS_POP_DEN4 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 72.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) as sum_hospital FROM BUILD_MAX_DEN WHERE lden>70 and lden<75 GROUP BY UUEID;")

    // final table
    sql.execute("CREATE TABLE "+outputDEN+" AS SELECT * FROM ROADS_POP_DEN5 UNION ALL SELECT UUEID, ROUND(SUM(pop),1) as sum_pop, 77.5 as range, SUM(BUILDING) as sum_building, SUM(SCHOOL) as sum_school, SUM(HOSPITAL) FROM BUILD_MAX_DEN WHERE lden>75 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputDEN+" ADD HA FLOAT;")
    sql.execute("UPDATE "+outputDEN+" SET HA = ROUND(sum_pop*(78.9270-3.1162*range+0.0342*range*range)/100,1) ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5;")

    // For CPI 
    // Taux d'incidence fixé à 0.00138
    logger.info("Compute CPI")
    sql.execute("DROP TABLE IF EXISTS CPI;")
    sql.execute("CREATE TABLE CPI AS SELECT b.UUEID, (SUM(b.sum_pop*((EXP((LN(1.08)/10)*(b.range-53)))-1))/(SUM(b.sum_pop*((EXP((LN(1.08)/10)*(b.range-53)))-1))+1))*0.00138*(SELECT SUM(a.sum_pop) from  "+outputDEN+" a WHERE a.UUEID = b.UUEID) as CPI FROM "+outputDEN+" b WHERE range > 53 GROUP BY UUEID;")


    sql.execute("CALL DBFWrite('data_dir/data/shapefiles/"+outputDEN+".dbf', '"+outputDEN.toUpperCase(Locale.ROOT)+"');")
    sql.execute("CALL DBFWrite('data_dir/data/shapefiles/"+outputNIGHT+".dbf', '"+outputNIGHT.toUpperCase(Locale.ROOT)+"');")
    sql.execute("CALL DBFWrite('data_dir/data/shapefiles/CPI.dbf', 'CPI');")
    // 
    // Remove non-needed tables
    //sql.execute("DROP TABLE IF EXISTS LDEN_GEOM_INFRA, LNIGHT_GEOM_INFRA, RECEIVERS_SUM_LAEQPA_DEN, RECEIVERS_SUM_LAEQPA_NIGHT, RECEIVERS_BUILD_NIGHT, RECEIVERS_BUILD_DEN, RECEIVERS_BUILD_POP_NIGHT, RECEIVERS_BUILD_POP_DEN, BUILD_MAX_NIGHT, BUILD_MAX_DEN;")

    resultString = "The table "+outputDEN+" and "+outputNIGHT+" have been created"
    logger.info(resultString)
    return resultString
}