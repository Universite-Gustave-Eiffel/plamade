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

package org.noise_planet.noisemodelling.wps.plamade

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

    // Define the ratio between population and dwellings
    def row_zone = sql.firstRow("SELECT * FROM METADATA")
    double ratioPopLog = row_zone.ratio_pop_log.toDouble()
    String nuts = row_zone.nuts.toString()



    // Création de la table de référence qui liste l'ensemble des classes d'iso attendues en fonction de l'indice et de la zone
    // Cette table servira au moment d'affecter les classes d'iso manquantes pour chaque UUEID

    sql.execute("DROP TABLE IF EXISTS ref_expo_noiseLevel;")
    sql.execute("CREATE TABLE ref_expo_noiseLevel (indice varchar, exposuretype varchar, noiseLevel varchar, ferconv boolean);")
    sql.execute("INSERT INTO ref_expo_noiseLevel (indice, exposuretype, noiseLevel, ferconv) VALUES ('LDEN', 'mostExposedFacade', 'Lden5559',false), ('LDEN', 'mostExposedFacade', 'Lden6064',false), ('LDEN', 'mostExposedFacade', 'Lden6569',false), ('LDEN', 'mostExposedFacade', 'Lden7074',false), ('LDEN', 'mostExposedFacade', 'LdenGreaterThan75',false), ('LNIGHT', 'mostExposedFacade', 'Lnight5054',false), ('LNIGHT', 'mostExposedFacade', 'Lnight5559',false), ('LNIGHT', 'mostExposedFacade', 'Lnight6064',false), ('LNIGHT', 'mostExposedFacade', 'Lnight6569',false), ('LNIGHT', 'mostExposedFacade', 'LnightGreaterThan70',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden55',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden65',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden75',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden5559',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden6064',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden6569',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'Lden7074',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'LdenGreaterThan75',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'LdenGreaterThan68',false), ('LDEN', 'mostExposedFacadeIncludingAgglomeration', 'LdenGreaterThan73',true), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'Lnight5054',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'Lnight5559',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'Lnight6064',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'Lnight6569',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'LnightGreaterThan70',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'LnightGreaterThan62',false), ('LNIGHT', 'mostExposedFacadeIncludingAgglomeration', 'LnightGreaterThan65', true);")

    // //////////////////////////////////////////////////////
    // Début des traitements

    logger.info("Count buildings, schools and hospitals")

    sql.execute("DROP TABLE IF EXISTS BUILDING_SCHOOL, BUILDING_HOSPITAL, BUILDING_COUNT_1, BUILDING_COUNT;")
    sql.execute("CREATE TABLE BUILDING_SCHOOL (BUILD_PK varchar PRIMARY KEY, ID_BAT varchar, ID_ERPS varchar, SCHOOL Integer) AS SELECT PK, ID_BAT, ID_ERPS, 1 FROM BUILDINGS_SCREENS WHERE ERPS_NATUR='Enseignement';")
    sql.execute("CREATE TABLE BUILDING_HOSPITAL (BUILD_PK varchar PRIMARY KEY, ID_BAT varchar, ID_ERPS varchar, HOSPITAL Integer) AS SELECT PK, ID_BAT, ID_ERPS, 1 FROM BUILDINGS_SCREENS WHERE ERPS_NATUR='Sante';")
    sql.execute("CREATE TABLE BUILDING_COUNT_1 (BUILD_PK varchar PRIMARY KEY, ID_BAT varchar, AGGLO boolean, BUILDING integer, SCHOOL integer, ID_ERPS_SCHOOL varchar) AS SELECT a.PK, a.ID_BAT, a.AGGLO, 1, b.SCHOOL, b.ID_ERPS FROM BUILDINGS_SCREENS a LEFT JOIN BUILDING_SCHOOL b ON a.PK=b.BUILD_PK;")
    sql.execute("CREATE TABLE BUILDING_COUNT (BUILD_PK varchar PRIMARY KEY, ID_BAT varchar, AGGLO boolean, BUILDING integer, SCHOOL integer, ID_ERPS_SCHOOL varchar, HOSPITAL integer, ID_ERPS_HOSP varchar) AS SELECT a.*, b.HOSPITAL, b.ID_ERPS FROM BUILDING_COUNT_1 a LEFT JOIN BUILDING_HOSPITAL b ON a.BUILD_PK=b.BUILD_PK;")
    sql.execute("DROP TABLE IF EXISTS BUILDING_SCHOOL, BUILDING_HOSPITAL, BUILDING_COUNT_1;")


    logger.info("Start computing working tables");

    sql.execute("DROP TABLE IF EXISTS LDEN_GEOM_INFRA, LNIGHT_GEOM_INFRA, RECEIVERS_SUM_LAEQPA_DEN, RECEIVERS_SUM_LAEQPA_NIGHT, RECEIVERS_BUILD_NIGHT, RECEIVERS_BUILD_DEN, RECEIVERS_BUILD_POP_NIGHT, RECEIVERS_BUILD_POP_DEN, BUILD_MAX_NIGHT, BUILD_MAX_NOT_AGGLO_NIGHT, BUILD_MAX_DEN, BUILD_MAX_NOT_AGGLO_DEN;")

    sql.execute("CREATE TABLE LDEN_GEOM_INFRA AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID, b.pk FROM " + tableDEN + " a, "+source+" b WHERE a.idsource=b.pk;")
    sql.execute("CREATE TABLE LNIGHT_GEOM_INFRA AS SELECT a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.UUEID, b.pk FROM " + tableNIGHT + " a, "+source+" b WHERE a.idsource=b.pk;")
    
    sql.execute("CREATE TABLE RECEIVERS_SUM_LAEQPA_DEN AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LDEN_GEOM_INFRA GROUP BY idreceiver, UUEID  ;")
    sql.execute("CREATE TABLE RECEIVERS_SUM_LAEQPA_NIGHT AS SELECT UUEID, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LNIGHT_GEOM_INFRA GROUP BY idreceiver, UUEID  ;")

    sql.execute("CREATE TABLE RECEIVERS_BUILD_DEN AS SELECT aden.UUEID, b.PK_1 as PK, aden.laeqpa_sum as lden FROM RECEIVERS_SUM_LAEQPA_DEN aden, receivers b WHERE aden.idreceiver=b.PK AND RCV_TYPE=1;")
    sql.execute("CREATE TABLE RECEIVERS_BUILD_NIGHT AS SELECT an.UUEID, b.PK_1 as PK, an.laeqpa_sum as lnight FROM RECEIVERS_SUM_LAEQPA_NIGHT an, receivers b WHERE an.idreceiver=b.PK AND RCV_TYPE=1 ;")

    sql.execute("CREATE TABLE RECEIVERS_BUILD_POP_DEN AS SELECT aden.UUEID, aden.PK, b.BUILD_PK, b.pop as POP, aden.lden as lden FROM RECEIVERS_BUILD_DEN aden, receivers_BUILDING b WHERE aden.PK=b.PK ;")
    sql.execute("CREATE TABLE RECEIVERS_BUILD_POP_NIGHT AS SELECT an.UUEID, an.PK, b.BUILD_PK, b.pop as POP, an.lnight as lnight FROM RECEIVERS_BUILD_NIGHT an, receivers_BUILDING b WHERE an.PK=b.PK ;")


    // Pour tout (mostExposedFacadeIncludingAgglomeration)
    sql.execute("CREATE TABLE BUILD_MAX_DEN AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, max(a.lden)-3 as lden FROM RECEIVERS_BUILD_POP_DEN a GROUP BY a.UUEID, a.BUILD_PK;")
    sql.execute("CREATE TABLE BUILD_MAX_NIGHT AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lnight)-3 as lnight FROM RECEIVERS_BUILD_POP_NIGHT a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")

    //sql.execute("CREATE TABLE BUILD_MAX_DEN AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lden)-3 as lden FROM RECEIVERS_BUILD_POP_DEN a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")
    //sql.execute("CREATE TABLE BUILD_MAX_NIGHT AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lnight)-3 as lnight FROM RECEIVERS_BUILD_POP_NIGHT a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")
    

    
    // Pour les zones en dehors des agglos (mostExposedFacade --> AGGLO is false)
    sql.execute("CREATE TABLE BUILD_MAX_NOT_AGGLO_DEN AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lden)-3 as lden FROM RECEIVERS_BUILD_POP_DEN a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK AND b.AGGLO is false GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")
    sql.execute("CREATE TABLE BUILD_MAX_NOT_AGGLO_NIGHT AS SELECT a.UUEID, ROUND(SUM(a.POP),1) as POP, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL, max(a.lnight)-3 as lnight FROM RECEIVERS_BUILD_POP_NIGHT a, BUILDING_COUNT b WHERE a.BUILD_PK=b.BUILD_PK AND b.AGGLO is false GROUP BY a.UUEID, a.BUILD_PK, b.BUILDING, b.SCHOOL, b.HOSPITAL;")


    //sql.execute("CREATE TABLE BUILD_MAX_NIGHT AS SELECT UUEID, ROUND(SUM(pop),0) as POP, BUILD_PK, max(lnight) as lnight FROM RECEIVERS_BUILD_POP_NIGHT GROUP BY UUEID, BUILD_PK;")

    sql.execute("DROP TABLE IF EXISTS "+outputDEN+", "+outputDEN+"_INCLUDING, "+outputDEN+"_NOT_AGGLO, "+outputNIGHT+", "+outputNIGHT+"_INCLUDING, "+outputNIGHT+"_NOT_AGGLO;")


    // /////////////////////////////////////////////////////
    // For NIGHT 

    // LNIGHT classes : 50-54, 55-59, 60-64, 65-69, >70, >62

    logger.info("Start computing LNIGHT tables")


    // Pour tout (mostExposedFacadeIncludingAgglomeration)
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5, ROADS_POP_NIGHT6;")

    // Classes Type A
    sql.execute("CREATE TABLE ROADS_POP_NIGHT1 AS SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 0.0 as range, 'remove' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight<50 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT2 AS SELECT * FROM ROADS_POP_NIGHT1 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 52.5 as range, 'Lnight5054' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=50 AND lnight<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT3 AS SELECT * FROM ROADS_POP_NIGHT2 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 57.5 as range, 'Lnight5559' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=55 and lnight<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT4 AS SELECT * FROM ROADS_POP_NIGHT3 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 62.5 as range, 'Lnight6064' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=60 and lnight<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT5 AS SELECT * FROM ROADS_POP_NIGHT4 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 67.5 as range, 'Lnight6569' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=65 and lnight<70 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT6 AS SELECT * FROM ROADS_POP_NIGHT5 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 72.5 as range, 'LnightGreaterThan70' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=65 and lnight>=70 GROUP BY UUEID;")

    // Classe Type C
    sql.execute("CREATE TABLE "+outputNIGHT+"_INCLUDING AS SELECT * FROM ROADS_POP_NIGHT6 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 62 as range, 'LnightGreaterThan62' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NIGHT WHERE lnight>=62 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputNIGHT+"_INCLUDING ADD HSD FLOAT;")
    sql.execute("UPDATE "+outputNIGHT+"_INCLUDING SET HSD = ROUND(exposedPeople*(19.4312-0.9336*range+0.0126*range*range)/100,1) ;")
    sql.execute("ALTER TABLE "+outputNIGHT+"_INCLUDING ADD EXPOSURETYPE VARCHAR;")
    sql.execute("UPDATE "+outputNIGHT+"_INCLUDING SET EXPOSURETYPE = 'mostExposedFacadeIncludingAgglomeration' ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5, ROADS_POP_NIGHT6;")


    // Pour les zones en dehors des agglos (mostExposedFacade --> AGGLO is false)
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5;")

    // Classes Type A
    sql.execute("CREATE TABLE ROADS_POP_NIGHT1 AS SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 0.0 as range, 'remove' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight<50 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT2 AS SELECT * FROM ROADS_POP_NIGHT1 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 52.5 as range, 'Lnight5054' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight>=50 AND lnight<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT3 AS SELECT * FROM ROADS_POP_NIGHT2 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 57.5 as range, 'Lnight5559' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight>=55 and lnight<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT4 AS SELECT * FROM ROADS_POP_NIGHT3 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 62.5 as range, 'Lnight6064' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight>=60 and lnight<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT5 AS SELECT * FROM ROADS_POP_NIGHT4 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 67.5 as range, 'Lnight6569' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight>=65 and lnight<70 GROUP BY UUEID;")

    sql.execute("CREATE TABLE "+outputNIGHT+"_NOT_AGGLO AS SELECT * FROM ROADS_POP_NIGHT5 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 72.5 as range, 'LnightGreaterThan70' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_NIGHT WHERE lnight>=70 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputNIGHT+"_NOT_AGGLO ADD HSD FLOAT;")
    sql.execute("UPDATE "+outputNIGHT+"_NOT_AGGLO SET HSD = ROUND(exposedPeople*(19.4312-0.9336*range+0.0126*range*range)/100,1) ;")
    sql.execute("ALTER TABLE "+outputNIGHT+"_NOT_AGGLO ADD EXPOSURETYPE VARCHAR;")
    sql.execute("UPDATE "+outputNIGHT+"_NOT_AGGLO SET EXPOSURETYPE = 'mostExposedFacade' ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT1, ROADS_POP_NIGHT2, ROADS_POP_NIGHT3, ROADS_POP_NIGHT4, ROADS_POP_NIGHT5;")


    sql.execute("CREATE TABLE "+outputNIGHT+" AS SELECT * FROM "+outputNIGHT+"_INCLUDING UNION ALL SELECT * FROM "+outputNIGHT+"_NOT_AGGLO;")

    sql.execute("ALTER TABLE "+outputNIGHT+" ADD PK VARCHAR;")
    sql.execute("UPDATE "+outputNIGHT+" SET PK =CONCAT(UUEID, '_', noiseLevel);")

    sql.execute("ALTER TABLE "+outputNIGHT+" ADD ESTATUnitCode VARCHAR;")
    sql.execute("UPDATE "+outputNIGHT+" SET ESTATUnitCode = '$nuts';")

    // //////////////////////////////////
    // Ajout des classes manquantes (car pas d'iso correspondantes)

    // Liste des classes devant exister (celles déjà présentes + celles manquantes), par UUEID
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_NIGHT_UUEID, ROADS_POP_NIGHT_UUEID_NOISE;")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT_UUEID as SELECT distinct UUEID FROM "+outputNIGHT+";")
    sql.execute("CREATE TABLE ROADS_POP_NIGHT_UUEID_NOISE as SELECT a.*, b.noiselevel, b.exposuretype FROM ROADS_POP_NIGHT_UUEID a, REF_EXPO_NOISELEVEL b WHERE INDICE='LNIGHT' AND FERCONV is false ORDER BY UUEID;")

    // Rassemblement dans une table des classes existantes et des manquantes
    // HSD est forcé à 99.99 pour que le champ soit créé en tant que double
    sql.execute("DROP TABLE IF EXISTS ROADS_EXPO_LNIGHT;")
    sql.execute("CREATE TABLE ROADS_EXPO_LNIGHT AS SELECT b.PK, b.ESTATUNITCODE, a.UUEID, a.EXPOSURETYPE, a.NOISELEVEL, b.EXPOSEDPEOPLE, b.RANGE, b.NB_BUILDING, b.EXPOSEDDWELLINGS, b.EXPOSEDHOSPITALS, b.EXPOSEDSCHOOLS, 99.99 as HA, b.HSD FROM ROADS_POP_NIGHT_UUEID_NOISE a LEFT JOIN "+outputNIGHT+" b ON a.UUEID = b.UUEID and a.noiselevel=b.noiselevel and a.exposuretype = b.exposuretype;")

    // Mise à jour des champs vides
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET PK= CONCAT(UUEID, '_', noiseLevel) WHERE PK is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET ESTATUNITCODE= (SELECT NUTS FROM METADATA) WHERE ESTATUNITCODE is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET HA = null where HA = 99.99;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET HSD = 0 where HSD is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET EXPOSEDPEOPLE = 0 WHERE EXPOSEDPEOPLE is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET NB_BUILDING = 0 WHERE NB_BUILDING is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET EXPOSEDDWELLINGS = 0 WHERE EXPOSEDDWELLINGS is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET EXPOSEDHOSPITALS = 0 WHERE EXPOSEDHOSPITALS is null;")
    sql.execute("UPDATE ROADS_EXPO_LNIGHT SET EXPOSEDSCHOOLS = 0 WHERE EXPOSEDSCHOOLS is null;")

    // Ajout de la colonne CPI, qui est renseignée à Null par défaut (car pas de CPI la nuit)
    sql.execute("ALTER TABLE ROADS_EXPO_LNIGHT ADD COLUMN CPI double default null;")







    // /////////////////////////////////////////////////////
    // For DAY 

    // LDEN classes : 55-59, 60-64, 65-69, 70-74, >75, 55, 65, 75 et >68

    logger.info("Start computing LDEN tables")

    // Pour tout (mostExposedFacadeIncludingAgglomeration)
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5, ROADS_POP_DEN6, ROADS_POP_DEN7, ROADS_POP_DEN8, ROADS_POP_DEN9;")

    // Classes Type A
    sql.execute("CREATE TABLE ROADS_POP_DEN1 AS SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 0.0 as range, 'remove' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN2 AS SELECT * FROM ROADS_POP_DEN1 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 57.5 as range, 'Lden5559' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=55 AND lden<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN3 AS SELECT * FROM ROADS_POP_DEN2 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 62.5 as range, 'Lden6064' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=60 and lden<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN4 AS SELECT * FROM ROADS_POP_DEN3 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 67.5 as range, 'Lden6569' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=65 and lden<70 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN5 AS SELECT * FROM ROADS_POP_DEN4 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 72.5 as range, 'Lden7074' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=70 and lden<75 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN6 AS SELECT * FROM ROADS_POP_DEN5 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 77.5 as range, 'LdenGreaterThan75' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=75 GROUP BY UUEID;")

    // Ajout des 3 classes Lden55, Lden65 et Lden75 (cette classe étant en fait égale à LdenGreaterThan75)
    sql.execute("CREATE TABLE ROADS_POP_DEN7 AS SELECT * FROM ROADS_POP_DEN6 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 55 as range, 'Lden55' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=55 GROUP BY UUEID;")    
    sql.execute("CREATE TABLE ROADS_POP_DEN8 AS SELECT * FROM ROADS_POP_DEN7 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 65 as range, 'Lden65' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=65 GROUP BY UUEID;")    
    sql.execute("CREATE TABLE ROADS_POP_DEN9 AS SELECT * FROM ROADS_POP_DEN8 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 75 as range, 'Lden75' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_DEN WHERE lden>=75 GROUP BY UUEID;")    

    // Classe Type C
    sql.execute("CREATE TABLE "+outputDEN+"_INCLUDING AS SELECT * FROM ROADS_POP_DEN9 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 68 as range, 'LdenGreaterThan68' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) FROM BUILD_MAX_DEN WHERE lden>=68 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputDEN+"_INCLUDING ADD HA FLOAT;")
    sql.execute("UPDATE "+outputDEN+"_INCLUDING SET HA = ROUND(exposedPeople*(78.9270-3.1162*range+0.0342*range*range)/100,1) ;")
    sql.execute("ALTER TABLE "+outputDEN+"_INCLUDING ADD EXPOSURETYPE VARCHAR;")
    sql.execute("UPDATE "+outputDEN+"_INCLUDING SET EXPOSURETYPE = 'mostExposedFacadeIncludingAgglomeration' ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5, ROADS_POP_DEN6, ROADS_POP_DEN7, ROADS_POP_DEN8, ROADS_POP_DEN9;")


    // Pour les zones en dehors des agglos (mostExposedFacade --> AGGLO is false)
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5;")

    // Classes Type A
    sql.execute("CREATE TABLE ROADS_POP_DEN1 AS SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 0.0 as range, 'remove' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden<55 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN2 AS SELECT * FROM ROADS_POP_DEN1 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 57.5 as range, 'Lden5559' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden>=55 AND lden<60 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN3 AS SELECT * FROM ROADS_POP_DEN2 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 62.5 as range, 'Lden6064' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden>=60 and lden<65 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN4 AS SELECT * FROM ROADS_POP_DEN3 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 67.5 as range, 'Lden6569' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden>=65 and lden<70 GROUP BY UUEID;")
    sql.execute("CREATE TABLE ROADS_POP_DEN5 AS SELECT * FROM ROADS_POP_DEN4 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 72.5 as range, 'Lden7074' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) as exposedHospitals FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden>=70 and lden<75 GROUP BY UUEID;")

    sql.execute("CREATE TABLE "+outputDEN+"_NOT_AGGLO AS SELECT * FROM ROADS_POP_DEN5 UNION ALL SELECT UUEID, ROUND(SUM(pop),0) as exposedPeople, 77.5 as range, 'LdenGreaterThan75' as noiseLevel, SUM(BUILDING) as nb_building, ROUND(SUM(POP)/"+ratioPopLog+",0) as exposedDwellings, SUM(SCHOOL) as exposedSchools, SUM(HOSPITAL) FROM BUILD_MAX_NOT_AGGLO_DEN WHERE lden>=75 GROUP BY UUEID;")
    sql.execute("ALTER TABLE "+outputDEN+"_NOT_AGGLO ADD HA FLOAT;")
    sql.execute("UPDATE "+outputDEN+"_NOT_AGGLO SET HA = ROUND(exposedPeople*(78.9270-3.1162*range+0.0342*range*range)/100,1) ;")
    sql.execute("ALTER TABLE "+outputDEN+"_NOT_AGGLO ADD EXPOSURETYPE VARCHAR;")
    sql.execute("UPDATE "+outputDEN+"_NOT_AGGLO SET EXPOSURETYPE = 'mostExposedFacade' ;")

    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN1, ROADS_POP_DEN2, ROADS_POP_DEN3, ROADS_POP_DEN4, ROADS_POP_DEN5;")

    sql.execute("CREATE TABLE "+outputDEN+" AS SELECT * FROM "+outputDEN+"_INCLUDING UNION ALL SELECT * FROM "+outputDEN+"_NOT_AGGLO;")
    sql.execute("ALTER TABLE "+outputDEN+" ADD PK VARCHAR;")
    sql.execute("UPDATE "+outputDEN+" SET PK =CONCAT(UUEID, '_', noiseLevel);")

    sql.execute("ALTER TABLE "+outputDEN+" ADD ESTATUnitCode VARCHAR;")
    sql.execute("UPDATE "+outputDEN+" SET ESTATUnitCode = '$nuts';")


    // //////////////////////////////////
    // CPI 
    // Taux d'incidence fixé à 0.00138
    logger.info("Compute CPI")
    sql.execute("DROP TABLE IF EXISTS CPI;")
    sql.execute("CREATE TABLE CPI AS SELECT b.UUEID, (SUM(b.exposedPeople*((EXP((LN(1.08)/10)*(b.range-53)))-1))/(SUM(b.exposedPeople*((EXP((LN(1.08)/10)*(b.range-53)))-1))+1))*0.00138*(SELECT SUM(a.exposedPeople) from  "+outputDEN+" a WHERE a.UUEID = b.UUEID) as CPI FROM "+outputDEN+" b WHERE range > 53 GROUP BY UUEID;")



    // //////////////////////////////////
    // Ajout des classes manquantes (car pas d'iso correspondantes)

    // Liste des classes devant exister (celles déjà présentes + celles manquantes), par UUEID
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN_UUEID, ROADS_POP_DEN_UUEID_NOISE;")
    sql.execute("CREATE TABLE ROADS_POP_DEN_UUEID as SELECT distinct UUEID FROM "+outputDEN+";")
    sql.execute("CREATE TABLE ROADS_POP_DEN_UUEID_NOISE as SELECT a.*, b.noiselevel, b.exposuretype FROM ROADS_POP_DEN_UUEID a, REF_EXPO_NOISELEVEL b WHERE INDICE='LDEN' AND FERCONV is false ORDER BY UUEID;")

    // Rassemblement dans une table des classes existantes et des manquantes
    // HSD est forcé à 99.99 pour que le champ soit créé en tant que double
    sql.execute("DROP TABLE IF EXISTS ROADS_EXPO_LDEN;")
    sql.execute("CREATE TABLE ROADS_EXPO_LDEN AS SELECT b.PK, b.ESTATUNITCODE, a.UUEID, a.EXPOSURETYPE, a.NOISELEVEL, b.EXPOSEDPEOPLE, b.RANGE, b.NB_BUILDING, b.EXPOSEDDWELLINGS, b.EXPOSEDHOSPITALS, b.EXPOSEDSCHOOLS, b.HA, 99.99 as HSD FROM ROADS_POP_DEN_UUEID_NOISE a LEFT JOIN "+outputDEN+" b ON a.UUEID = b.UUEID and a.noiselevel=b.noiselevel and a.exposuretype = b.exposuretype;")

    // Mise à jour des champs vides
    sql.execute("UPDATE ROADS_EXPO_LDEN SET PK= CONCAT(UUEID, '_', noiseLevel) WHERE PK is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET ESTATUNITCODE= (SELECT NUTS FROM METADATA) WHERE ESTATUNITCODE is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET HA = 0 where HA is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET HSD = null where HSD = 99.99;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET EXPOSEDPEOPLE = 0 WHERE EXPOSEDPEOPLE is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET NB_BUILDING = 0 WHERE NB_BUILDING is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET EXPOSEDDWELLINGS = 0 WHERE EXPOSEDDWELLINGS is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET EXPOSEDHOSPITALS = 0 WHERE EXPOSEDHOSPITALS is null;")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET EXPOSEDSCHOOLS = 0 WHERE EXPOSEDSCHOOLS is null;")

    // Ajout du CPI
    sql.execute("ALTER TABLE ROADS_EXPO_LDEN ADD COLUMN CPI double;")
    sql.execute("UPDATE ROADS_EXPO_LDEN a SET a.CPI = (SELECT b.CPI from CPI b WHERE a.UUEID=b.UUEID);")
    sql.execute("UPDATE ROADS_EXPO_LDEN SET CPI = null WHERE NOISELEVEL='LdenGreaterThan68';")




    // ////////////////////////////////////////////////
    // Unification des tables LDEN et LNIGHT pour la route
    sql.execute("DROP TABLE IF EXISTS ROADS_UNIFY, ROADS_EXPO;")
    sql.execute("CREATE TABLE ROADS_UNIFY AS SELECT * FROM ROADS_EXPO_LDEN UNION ALL SELECT * FROM ROADS_EXPO_LNIGHT;")
    sql.execute("CREATE TABLE ROADS_EXPO AS SELECT * FROM ROADS_UNIFY ORDER BY UUEID, NOISELEVEL;")




    // 
    // Remove non-needed tables
    //sql.execute("DROP TABLE IF EXISTS LDEN_GEOM_INFRA, LNIGHT_GEOM_INFRA, RECEIVERS_SUM_LAEQPA_DEN, RECEIVERS_SUM_LAEQPA_NIGHT, RECEIVERS_BUILD_NIGHT, RECEIVERS_BUILD_DEN, RECEIVERS_BUILD_POP_NIGHT, RECEIVERS_BUILD_POP_DEN, BUILD_MAX_NIGHT, BUILD_MAX_DEN;")
    sql.execute("DROP TABLE IF EXISTS ROADS_POP_DEN_INCLUDING, ROADS_POP_DEN_NOT_AGGLO, ROADS_POP_NIGHT_INCLUDING, ROADS_POP_NIGHT_NOT_AGGLO, ROADS_POP_DEN_UUEID, ROADS_POP_DEN_UUEID_NOISE, ROADS_POP_NIGHT_UUEID, ROADS_POP_NIGHT_UUEID_NOISE, ROADS_POP_DEN, ROADS_POP_NIGHT, ROADS_UNIFY, REF_EXPO_NOISELEVEL;")



    resultString = "The table ROADS_EXPO has been created"
    logger.info(resultString)
    return resultString
}













