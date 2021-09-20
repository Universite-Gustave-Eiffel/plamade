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
    - Merge 3D lines topo with BD Alti
    - Confirm that screens are taken 2 times into account for railway
    - Check spatial index and srids
 */ 

package org.noise_planet.noisemodelling.wps.plamade

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.text.SimpleTemplateEngine
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.locationtech.jts.geom.Point
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.sql.Sql
import java.sql.Connection
import java.sql.Statement
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation

title = 'Extract department'
description = 'Connect to a distant PostGIS database and extract departments according to Plamade specification'

inputs = [
        databaseUser : [
                name       : 'PostGIS user',
                title      : 'PostGIS username',
                description: 'PostGIS username for authentication',
                type       : String.class
        ],
        databasePassword : [
                name       : 'PostGIS password',
                title      : 'PostGIS password',
                description: 'PostGIS password for authentication',
                type       : String.class
        ],
        fetchDistance : [
                name       : 'Fetch distance',
                title      : 'Fetch distance',
                description: 'Fetch distance around the selected area in meters. Default 1000',
                min : 0, max: 1000,
                type       : Integer.class
        ],
        inseeDepartment : [
                name       : 'Insee department code',
                title      : 'Insee department code',
                description: 'Insee code for the area ex:75',
                type       : String.class
        ],
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


    //------------------------------------------------------
    // Clean the database before starting the importation

    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

    // Build the result string with every tables
    StringBuilder sb = new StringBuilder()

    // Get every table names
    List<String> tables = JDBCUtilities.getTableNames(connection.getMetaData(), null, "PUBLIC", "%", null)

    // Loop over the tables
    tables.each { t ->
        TableLocation tab = TableLocation.parse(t)
            if (!ignorelst.contains(tab.getTable())) {
                // Add the name of the table in the string builder
                if (sb.size() > 0) {
                    sb.append(" || ")
                }
                sb.append(tab.getTable())
                // Create a connection statement to interact with the database in SQL
                Statement stmt = connection.createStatement()
                // Drop the table
                stmt.execute("drop table if exists " + tab)
            }
    }

    //------------------------------------------------------


    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    ProgressVisitor progressVisitor

    if("progressVisitor" in input) {
        progressVisitor = input["progressVisitor"] as ProgressVisitor
    } else {
        progressVisitor = new RootProgressVisitor(1, true, 1);
    }

    ProgressVisitor progress = progressVisitor.subProcess(11)
    // print to command window
    logger.info('Start linking with PostGIS')

    // Get provided parameters
    String codeDep = input["inseeDepartment"] as String

    String codeDepFormat = codeDep.size() == 2 ? "0$codeDep" : codeDep


    Integer buffer = 1000
    if ('fetchDistance' in input) {
        buffer = input["fetchDistance"] as Integer
    }

    def databaseUrl = "jdbc:postgresql_h2://57.100.98.126:5432/plamade?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
    // def databaseUrl = "jdbc:postgresql_h2://plamade.noise-planet.org:5433/plamade_2021_05_03?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
    //def databaseUrl = input["databaseUrl"] as String
    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String

    // Declare table variables depending on the department and the projection system
    def srid = "2154"
    def table_station = "station_pfav_2154"
    def table_dept = "departement_2154"
    def table_route = "N_ROUTIER_TRONCON_L_2154" 
    def table_rail = "N_FERROVIAIRE_TRONCON_L_2154"
    def table_bati = "C_BATIMENT_S_2154"
    def table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154"
    def table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154"
    def table_land = "C_NATURESOL_S_2154"
    def table_infra = "infra_2154"

    if(codeDep=='971' || codeDep=='972') {
        srid="5490"
        table_station = "station_pfav_5490"
        table_dept = "departement_5490"
        table_route = "N_ROUTIER_TRONCON_L_5490"
        table_rail = "N_FERROVIAIRE_TRONCON_L_5490"
        table_bati = "C_BATIMENT_S_5490"
        table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_5490"
        table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_5490"
        table_land = "C_NATURESOL_S_5490"
        table_infra = "infra_5490"
    }
    else if(codeDep=='973') {
        srid="2972"
        table_station = "station_pfav_2972"
        table_dept = "departement_2972"
        table_route = "N_ROUTIER_TRONCON_L_2972"
        table_rail = "N_FERROVIAIRE_TRONCON_L_2972"
        table_bati = "C_BATIMENT_S_2972"
        table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2972"
        table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2972"
        table_land = "C_NATURESOL_S_2972"
        table_infra = "infra_2972"
    }
    else if(codeDep=='974') {
        srid="2975"
        table_station = "station_pfav_2975"
        table_dept = "departement_2975"
        table_route = "N_ROUTIER_TRONCON_L_2975"
        table_rail = "N_FERROVIAIRE_TRONCON_L_2975"
        table_bati = "C_BATIMENT_S_2975"
        table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2975"
        table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2975"
        table_land = "C_NATURESOL_S_2975"
        table_infra = "infra_2975"
    }
    else if(codeDep=='976') {
        srid="4471"
        table_station = "station_pfav_4471"
        table_dept = "departement_4471"
        table_route = "N_ROUTIER_TRONCON_L_4471"
        table_rail = "N_FERROVIAIRE_TRONCON_L_4471"
        table_bati = "C_BATIMENT_S_4471"
        table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_4471"
        table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_4471"
        table_land = "C_NATURESOL_S_4471"
        table_infra = "infra_4471"
    }


    def sql = new Sql(connection)

    def queries_conf = """
    ----------------------------------
    -- Manage metadata tables

    DROP TABLE IF EXISTS nuts_link, metadata;
    CREATE LINKED TABLE nuts_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT code_2021 as nuts FROM noisemodelling.nuts WHERE code_dept=''$codeDepFormat'')');

    CREATE TABLE metadata (code_dept varchar, nuts varchar, srid integer, import_start timestamp, import_end timestamp, 
        grid_conf integer, grid_start timestamp, grid_end timestamp, 
        emi_conf integer, emi_start timestamp, emi_end timestamp, 
        road_conf integer, road_start timestamp, road_end timestamp, 
        rail_conf integer, rail_start timestamp, rail_end timestamp);

    INSERT INTO metadata (code_dept, nuts, srid, import_start) VALUES ('$codeDep', (SELECT nuts from nuts_link), $srid, NOW());
    
    DROP TABLE nuts_link;

    ----------------------------------
    -- Manage configuration tables

    -- CONF
    DROP TABLE IF EXISTS conf_link, conf;
    CREATE LINKED TABLE conf_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT * FROM noisemodelling.conf)');
    CREATE TABLE conf as select * FROM conf_link;
    DROP TABLE conf_link;
    
    -- CONF_ROAD
    DROP TABLE IF EXISTS conf_road_link, conf_road;
    CREATE LINKED TABLE conf_road_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT * FROM noisemodelling.conf_road)');
    CREATE TABLE conf_road as select * FROM conf_road_link;
    DROP TABLE conf_road_link;
    
    -- CONF_RAIL
    DROP TABLE IF EXISTS conf_rail_link, conf_rail;
    CREATE LINKED TABLE conf_rail_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT * FROM noisemodelling.conf_rail)');
    CREATE TABLE conf_rail as select * FROM conf_rail_link;
    DROP TABLE conf_rail_link;

    -- PLATEFORME
    DROP TABLE IF EXISTS plateform_link, plateform;
    CREATE LINKED TABLE plateform_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT * FROM noisemodelling.platform)');
    CREATE TABLE plateform as select * FROM plateform_link;
    DROP TABLE plateform_link;

    """
    def queries_pfav = """       
    ----------------------------------
    -- Manage pfav and meteo tables

    DROP TABLE IF EXISTS dept_pfav;
    CREATE LINKED TABLE dept_pfav ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT b.insee_dep, a.* 
        FROM noisemodelling.$table_station a, noisemodelling.$table_dept b 
        WHERE insee_dep=''$codeDep'' ORDER BY st_distance(a.the_geom, st_centroid(b.the_geom)) LIMIT 1)');
        
    DROP TABLE IF EXISTS dept_meteo;
    CREATE LINKED TABLE dept_meteo ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT "CODEDEPT" as codedept, "TEMP_D" as temp_d, "TEMP_E" as temp_e, "TEMP_N" as temp_n, 
        "HYGRO_D" as hygro_d, "HYGRO_E" as hygro_e, "HYGRO_N" as hygro_n, "TS_STUD" as ts_stud, "PM_STUD" as pm_stud 
        FROM echeance4."C_METEO_S" 
        WHERE "CODEDEPT"=lpad(''$codeDep'',3,''0''))');
    
    DROP TABLE IF EXISTS zone;
    CREATE TABLE zone AS SELECT 
        a.pfav_06_18, a.pfav_18_22, a.pfav_22_06, a.pfav_06_22, 
        b.temp_d, b.temp_e, b.temp_n, b.hygro_d, b.hygro_e, b.hygro_n, b.ts_stud, b.pm_stud 
        FROM dept_pfav a, dept_meteo b;
    
    DROP TABLE IF EXISTS dept_pfav,dept_meteo;

    """
    def queries_roads = """
	----------------------------------
    -- Manage roads

    DROP TABLE IF EXISTS roads_link, roads, pvmt_link;

    CREATE LINKED TABLE roads_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT  st_translate(st_force3dz(a.the_geom), 0, 0, 0.05) as the_geom, 
     a."IDTRONCON" as id_troncon, a."IDROUTE" as id_route, f."NOMROUTE" as nom_route,
     b."TMHVLD" as lv_d, b."TMHVLS" as lv_e, b."TMHVLN" as lv_n, b."TMHPLD" * b."PCENTMPL" as mv_d,b."TMHPLS" * b."PCENTMPL" as mv_e, 
     b."TMHPLN" * b."PCENTMPL" as mv_n, b."TMHPLD" * b."PCENTHPL" as hgv_d, b."TMHPLS" * b."PCENTHPL" as hgv_e, b."TMHPLN" * b."PCENTHPL" as hgv_n, 
     b."TMH2RD" * b."PCENT2R4A" as wav_d,b."TMH2RS" * b."PCENT2R4A" as wav_e, b."TMH2RN" * b."PCENT2R4A" as wav_n, b."TMH2RD" * b."PCENT2R4B" as wbv_d, 
     b."TMH2RS" * b."PCENT2R4B" as wbv_e,b."TMH2RN" * b."PCENT2R4B" as wbv_n, c."VITESSEVL" as lv_spd_d, c."VITESSEVL" as lv_spd_e, 
     c."VITESSEVL" as lv_spd_n, c."VITESSEPL" as mv_spd_d,c."VITESSEPL" as mv_spd_e, c."VITESSEPL" as mv_spd_n, c."VITESSEPL" as hgv_spd_d, 
     c."VITESSEPL" as hgv_spd_e, c."VITESSEPL" as hgv_spd_n, c."VITESSE4A" as wav_spd_d, c."VITESSE4A" as wav_spd_e, c."VITESSE4A" as wav_spd_n, 
     c."VITESSE4B" as wbv_spd_d, c."VITESSE4B" as wbv_spd_e, c."VITESSE4B" as wbv_spd_n,
     d."REVETEMENT" as revetement,
     d."GRANULO" as granulo,
     d."CLASSACOU" as classacou,
     a."NB_VOIES" as ntrack,
     a."LARGEUR" as width,
     a."ZDEB" as z_start,
     ST_Z(ST_StartPoint(a.the_geom)) as z_debut, 
     a."ZFIN" as z_end,
     ROUND((a."ZFIN"-a."ZDEB")/ ST_LENGTH(a.the_geom)*100) as slope,
     (CASE  WHEN a."SENS" = ''01'' THEN ''01'' 
       WHEN a."SENS" = ''02'' THEN ''02'' 
       ELSE ''03''
      END) as way,
     f."UUEID" as uueid,
     a."AGGLO" as agglo 
    FROM 
     noisemodelling."$table_route" a,
     echeance4."N_ROUTIER_TRAFIC" b,
     echeance4."N_ROUTIER_VITESSE" c,
     echeance4."N_ROUTIER_REVETEMENT" d, 
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e,
     echeance4."N_ROUTIER_ROUTE" f 
    WHERE 
     ST_LENGTH(a.the_geom)>0 and
     a."CBS_GITT" and
     f."CONCESSION"=''N'' and 
     a."IDTRONCON"=b."IDTRONCON" and
     a."IDTRONCON"=c."IDTRONCON" and
     a."IDTRONCON"=d."IDTRONCON" and 
     a."IDROUTE"=f."IDROUTE" and
     a.the_geom && e.the_geom and 
     ST_INTERSECTS(a.the_geom, e.the_geom))');
    
    CREATE TABLE ROADS AS SELECT * FROM roads_link;
    DROP TABLE roads_link;
    ALTER TABLE roads ADD COLUMN pvmt varchar(4);
    CREATE LINKED TABLE pvmt_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 'pvmt');
    DROP TABLE IF EXISTS PVMT;
    CREATE TABLE PVMT as select * from pvmt_link;
    DROP TABLE pvmt_link;
    CREATE INDEX ON PVMT (revetement);
    CREATE INDEX ON PVMT (granulo);
    CREATE INDEX ON PVMT (classacou);
    UPDATE roads b SET pvmt = (select a.pvmt FROM pvmt a WHERE a.revetement=b.revetement AND a.granulo=b.granulo AND a.classacou=b.classacou);
    ALTER TABLE roads ADD COLUMN pk serial PRIMARY KEY;
    CREATE spatial index ON roads (the_geom);
    CREATE INDEX ON ROADS(UUEID);

    """
    def queries_rails = """
    ----------------------------------
    -- Manage rail_sections

    DROP TABLE IF EXISTS rail_sections, rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel, rail_traffic_link, rail_traffic;

    CREATE LINKED TABLE rail_sections_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        a.the_geom, 
        a."IDTRONCON" as idsection,
        a."NB_VOIES" as ntrack, 
        a."IDLIGNE" as idline, 
        a."NUMLIGNE" as numline, 
        a."VMAXINFRA" as trackspd, 
        (CASE   WHEN a."BASEVOIE" = ''C'' THEN 5::integer 
                WHEN a."BASEVOIE" = ''W'' THEN 7::integer 
                WHEN a."BASEVOIE" = ''N'' THEN 7::integer
        END) as transfer,
        (CASE   WHEN a."RUGOSITE" = ''C'' THEN 1::integer 
                WHEN a."RUGOSITE" = ''H'' THEN 2::integer 
                WHEN a."RUGOSITE" = ''M'' and c."TYPELIGNE" = ''01'' THEN 1::integer 
                WHEN a."RUGOSITE" = ''M'' and c."TYPELIGNE" = ''02'' THEN 2::integer
        END) as roughness,
        (CASE   WHEN a."JOINTRAIL" = ''N'' THEN 0::integer 
                WHEN a."JOINTRAIL" = ''M'' THEN 1::integer 
        END) as impact,  
        (CASE  WHEN a."COURBURE" = ''N'' THEN 0::integer
        END) as curvature,
        (CASE  WHEN a."BASEVOIE" = ''N'' THEN 1::integer 
               ELSE 0::integer
        END) as bridge,
        a."LARGEMPRIS" - 5.5 as d2,
        a."LARGEMPRIS" - 4 as d3,
        a."LARGEMPRIS" as d4,
        b."VITESSE" as comspd,
        c."TYPELIGNE" as linetype,
        (CASE   WHEN c."TYPELIGNE" = ''01'' THEN 3.67::float 
                WHEN c."TYPELIGNE" = ''02'' THEN 4.5::float 
        END) as trackspc,
        c."UUEID" as uueid 
    FROM 
        noisemodelling."$table_rail" a,
        echeance4."N_FERROVIAIRE_VITESSE" b,
        echeance4."N_FERROVIAIRE_LIGNE" c, 
        (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"=''412280737'' and  
        a."IDTRONCON" = b."IDTRONCON" and
        a."IDLIGNE" = c."IDLIGNE" and 
        a.the_geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE rail_sections_geom AS SELECT * FROM rail_sections_link;

    ALTER TABLE rail_sections_geom ADD COLUMN bridgeopt integer DEFAULT 0;

    CREATE LINKED TABLE rail_tunnel_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTUNNEL" as idtunnel, 
        "IDTRONCON" as idsection 
    FROM 
        echeance4."N_FERROVIAIRE_TUNNEL")');

    CREATE TABLE rail_tunnel AS SELECT * FROM rail_tunnel_link;

    CREATE TABLE rail_sections AS SELECT ST_SETSRID(a.THE_GEOM,$srid) THE_GEOM,a.IDSECTION,a.NTRACK,a.IDLINE,a.NUMLINE,a.TRACKSPD,a.TRANSFER,a.ROUGHNESS,a.IMPACT,a.CURVATURE,a.BRIDGE,a.D2,a.D3,a.D4,a.COMSPD,a.LINETYPE,a.TRACKSPC,a.UUEID,a.BRIDGEOPT, b.idtunnel FROM rail_sections_geom a LEFT JOIN rail_tunnel b ON a.idsection = b.idsection;
    ALTER TABLE rail_sections ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX rail_sections_geom_idx ON rail_sections (the_geom);
    CREATE INDEX ON rail_sections (idsection);
    CREATE INDEX ON rail_sections(UUEID);

    -- Rail_traffic
    CREATE LINKED TABLE rail_traffic_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTRAFIC" as idtraffic,
        "IDTRONCON" as idsection,
        "VMAX" as trainspd,
        "ENGMOTEUR" as traintype,
        "TDIURNE" as tday,
        "TSOIR" as tevening,
        "TNUIT" as tnight 
    FROM 
        echeance4."N_FERROVIAIRE_TRAFIC")');

    CREATE TABLE rail_traffic AS SELECT a.* FROM rail_traffic_link a, rail_sections b WHERE a.idsection=b.idsection;
    ALTER TABLE rail_traffic ADD COLUMN pk serial PRIMARY KEY;

    DROP TABLE rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel, rail_traffic_link;

    """
    def queries_infra = """
    ----------------------------------
    -- Generate infrastructure table (merge of roads and rails)

    DROP TABLE IF EXISTS infra;
    CREATE TABLE infra AS SELECT the_geom FROM roads UNION ALL SELECT the_geom FROM rail_sections;
    CREATE SPATIAL INDEX infra_geom_idx ON infra (the_geom);

    """
    def queries_buildings = """
	----------------------------------
    -- Manage buildings

    DROP TABLE IF EXISTS allbuildings_link, buildings_geom, allbuildings_erps_link, allbuildings_erps, allbuildings_erps_natur_link, allbuildings_erps_natur, buildings_erps, buildings;
    
    CREATE LINKED TABLE allbuildings_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT 
     a.the_geom, 
     a."IDBAT" as id_bat, 
     a."BAT_UUEID" as bat_uueid,
     a."BAT_HAUT" as height,
     b."POP_BAT" as pop
    FROM 
     noisemodelling."$table_bati" a,
     echeance4."C_POPULATION" b,
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    where
     a.the_geom && c.the_geom and 
     ST_INTERSECTS(a.the_geom, c.the_geom) and 
     a."IDBAT"=b."IDBAT")');
    
    CREATE TABLE buildings_geom as SELECT * from allbuildings_link;
    CREATE SPATIAL INDEX ON buildings_geom(the_geom);
    CREATE INDEX ON buildings_geom(id_bat);
    DELETE FROM buildings_geom B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
        
    -- Get ERPS buildings list
	CREATE LINKED TABLE allbuildings_erps_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
	'(SELECT "IDBAT" as id_bat, "IDERPS" as id_erps FROM echeance4."C_CORRESPOND_BATIMENT_BATIMENTSENSIBLE")');

	-- Get ERPS nature
    CREATE LINKED TABLE allbuildings_erps_natur_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT "IDERPS" as id_erps, "ERPS_NATUR" as erps_natur FROM echeance4."C_BATIMENTSENSIBLE")');





    -- Save linked tables to be able to create indexes
    CREATE TABLE allbuildings_erps AS SELECT * FROM allbuildings_erps_link;
    CREATE TABLE allbuildings_erps_natur AS SELECT * FROM allbuildings_erps_natur_link;
    CREATE INDEX ON allbuildings_erps(id_erps);
    CREATE INDEX ON allbuildings_erps_natur(id_erps);


    -- Merge both ERPS informations
    CREATE TABLE buildings_erps as SELECT a.*, b.erps_natur from allbuildings_erps a, allbuildings_erps_natur b WHERE a.id_erps = b.id_erps;
    CREATE INDEX ON buildings_erps(id_bat);


	-- Merge both geom and ERPS tables into builings table
	CREATE TABLE buildings as SELECT a.the_geom, a.id_bat, a.bat_uueid, a.height, a.pop, b.id_erps, b.erps_natur FROM buildings_geom a LEFT JOIN buildings_erps b ON a.id_bat = b.id_bat;
    ALTER TABLE buildings ADD COLUMN pk serial PRIMARY KEY;
    ALTER TABLE buildings ADD COLUMN g float DEFAULT 0.1;
    ALTER TABLE buildings ADD COLUMN origin varchar DEFAULT 'building';
    CREATE SPATIAL INDEX ON buildings(the_geom);
    
	DROP TABLE buildings_geom, buildings_erps, allbuildings_link, allbuildings_erps_link, allbuildings_erps, allbuildings_erps_natur_link, allbuildings_erps_natur;

    """
    def queries_screens = """
    ----------------------------------
    -- Manage acoustic screens

    -- For roads

    DROP TABLE IF EXISTS road_screens_link, road_screens;

    CREATE LINKED TABLE road_screens_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT
        a.the_geom,
        a."IDPROTACOU" as id_bat,
        a."HAUTEUR" as height,
        a."PROPRIETE" as propriete,
        a."MATERIAU1" as materiau1
    FROM 
        noisemodelling."$table_route_protect" a,
        (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a.the_geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE road_screens AS SELECT * FROM road_screens_link;
    ALTER TABLE road_screens ADD COLUMN origin varchar DEFAULT 'road';

    DELETE FROM road_screens B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);

    -- For rail

    DROP TABLE IF EXISTS rail_screens_link, rail_screens;

    CREATE LINKED TABLE rail_screens_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT
        a.the_geom,
        a."IDPROTACOU" as id_bat,
        a."HAUTEUR" as height,
        a."PROPRIETE" as propriete,
        a."MATERIAU1" as materiau1        
    FROM 
        noisemodelling."$table_rail_protect" a,
        (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a.the_geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE rail_screens AS SELECT * FROM rail_screens_link;
    ALTER TABLE rail_screens ADD COLUMN origin varchar DEFAULT 'rail';

    DELETE FROM rail_screens B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);


    -- Merge both screens tables
    DROP TABLE IF EXISTS screens;
    CREATE TABLE screens AS SELECT * FROM road_screens UNION ALL SELECT * FROM rail_screens;

    ALTER TABLE screens ADD COLUMN bat_uueid varchar;
    ALTER TABLE screens ADD COLUMN g float DEFAULT 0;
    UPDATE screens SET g = 0.7 WHERE propriete = '01';
    UPDATE screens SET g = 0.7 WHERE (propriete = '00' or propriete = '99') AND (materiau1 = '01' or materiau1 = '04' or materiau1 = '06');

    ALTER TABLE screens ADD COLUMN pop integer DEFAULT 0;
    --ALTER TABLE screens ADD COLUMN id_erps integer DEFAULT 0;
    ALTER TABLE screens ADD COLUMN id_erps varchar;    
    ALTER TABLE screens ADD COLUMN erps_natur varchar;
    ALTER TABLE screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON screens(the_geom);

    DROP TABLE road_screens_link, road_screens, rail_screens_link, rail_screens;

    """
    def queries_buildings_screens = """
    ----------------------------------
    -- Merge buildings and screenss

    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buildings_screens;

    CREATE TABLE tmp_relation_screen_building AS SELECT b.pk as pk_building, s.pk as pk_screen 
        FROM buildings b, screens s 
        WHERE b.the_geom && s.the_geom AND ST_Distance(b.the_geom, s.the_geom) <= 0.5;

    -- For intersecting screens, remove parts closer than distance_truncate_screens
    CREATE TABLE tmp_screen_truncated AS SELECT pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 0.5)) the_geom, s.id_bat, s.bat_uueid, s.height, s.pop, s.id_erps, s.erps_natur, s.g, s.origin 
        FROM tmp_relation_screen_building r, buildings b, screens s 
        WHERE pk_building = b.pk AND pk_screen = s.pk 
        GROUP BY pk_screen, s.id_bat, s.bat_uueid, s.height, s.pop, s.id_erps, s.erps_natur, s.g, s.origin;

    -- Merge untruncated screens and truncated screens
    CREATE TABLE tmp_screens AS 
        SELECT the_geom, pk, id_bat, bat_uueid, height, pop, id_erps, erps_natur, g, origin FROM screens WHERE pk not in (SELECT pk_screen FROM tmp_screen_truncated) UNION ALL 
        SELECT the_geom, pk_screen as pk, id_bat, bat_uueid, height, pop, id_erps, erps_natur, g, origin FROM tmp_screen_truncated;

    -- Convert linestring screens to polygons with buffer function
    CREATE TABLE tmp_buffered_screens AS SELECT ST_SETSRID(ST_BUFFER(sc.the_geom, 0.1, 'join=mitre endcap=flat'), ST_SRID(sc.the_geom)) as the_geom, pk, id_bat, bat_uueid, height, pop, id_erps, erps_natur, g, origin 
        FROM tmp_screens sc;

    -- Merge buildings and buffered screens
    CREATE TABLE buildings_screens as 
        SELECT the_geom, id_bat, bat_uueid, height, pop, id_erps, erps_natur, g, origin FROM tmp_buffered_screens sc UNION ALL 
        SELECT the_geom, id_bat, bat_uueid, height, pop, id_erps, erps_natur, g, origin FROM buildings;

    ALTER TABLE buildings_screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON buildings_screens(the_geom);

    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buffered_screens;
    
    """
    def queries_landcover = """
	----------------------------------
	-- Manage Landcover
    
	DROP TABLE IF EXISTS alllandcover_link, landcover;
    CREATE LINKED TABLE alllandcover_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT 
     a.the_geom, 
     a."IDNATSOL" as pk, 
     a."NATSOL_LIB" as clc_lib,
     a."NATSOL_CNO" as g
    FROM 
     noisemodelling."$table_land" a,
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    WHERE
     a.the_geom && c.the_geom and 
     ST_INTERSECTS(a.the_geom, c.the_geom) and 
     a."NATSOL_CNO" > 0)');
    
    CREATE TABLE landcover as select * from alllandcover_link;
    CREATE SPATIAL INDEX ON landcover(the_geom);
    DELETE FROM landcover B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    DROP TABLE alllandcover_link;

    """
    def queries_dem = """
    ----------------------------------
    -- Manage DEM (import and filter within 1000m the BD Alti)

    DROP TABLE IF EXISTS bdalti_link, dem;
    CREATE LINKED TABLE bdalti_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT distinct b.* FROM bd_alti.pt3d_alti_d$codeDepFormat b, noisemodelling.$table_infra i WHERE ST_EXPAND(B.THE_GEOM, $buffer) && i.THE_GEOM AND ST_DISTANCE(b.the_geom, i.the_geom) < $buffer)');
    CREATE TABLE dem AS SELECT * FROM bdalti_link;
    DROP TABLE IF EXISTS bdalti_link;

    ----------------------------------
    -- Clean tables

    DROP TABLE PVMT;
    DROP TABLE INFRA;
    
    -- COPY BDTOPO roads (3 dimensions) for the studied area
    CREATE LINKED TABLE t_route_metro_corse ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.* FROM bd_topo.t_route_metro_corse r,
            (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where R.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, E.THE_GEOM) < 1000 AND st_zmin(R.THE_GEOM) > -1000)');
    
    -- Remove BDTOPO roads that are far from studied roads
    DROP TABLE IF EXISTS ROUTE_METRO_CORSE;
    create table ROUTE_METRO_CORSE as select * from t_route_metro_corse;
    delete from ROUTE_METRO_CORSE B WHERE NOT EXISTS (SELECT 1 FROM ROADS R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    
    drop table if exists t_route_metro_corse; 
    
    create spatial index on ROUTE_METRO_CORSE(the_geom);
 
    DROP TABLE DEM_WITHOUT_PTLINE IF EXISTS;
    CREATE TABLE DEM_WITHOUT_PTLINE AS SELECT d.the_geom FROM dem d;    
    DELETE FROM DEM_WITHOUT_PTLINE WHERE EXISTS (SELECT 1 FROM route_metro_corse b WHERE ST_EXPAND(DEM_WITHOUT_PTLINE.THE_GEOM,15, 15)   && b.the_geom AND ST_DISTANCE(DEM_WITHOUT_PTLINE.THE_GEOM, b.the_geom )< 15 LIMIT 1) ;
    ALTER TABLE route_metro_corse ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE route_metro_corse add primary key(pk_line);
    -- Create buffer points from roads and copy the elevation from the roads to the point
    DROP TABLE IF EXISTS BUFFERED_PTLINE;
    CREATE TABLE BUFFERED_PTLINE AS SELECT st_tomultipoint(st_densify(st_buffer(st_simplify(the_geom, 2), GREATEST(NB_VOIES, 1) * 3.5  ,'endcap=flat join=mitre'), 25 )) the_geom,  pk_line from route_metro_corse rmc;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM) SELECT ST_MAKEPOINT(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM FROM ST_EXPLODE('BUFFERED_PTLINE') P, route_metro_corse L WHERE P.PK_LINE = L.PK_LINE;
    CREATE SPATIAL INDEX ON DEM_WITHOUT_PTLINE (THE_GEOM);
    
    DROP TABLE IF EXISTS DEM;
    ALTER TABLE DEM_WITHOUT_PTLINE RENAME TO DEM;
    """




    def queries_stats = """
    ----------------------------------
    -- Manage statitics
    
    DROP TABLE IF EXISTS stat_road_fr_link, stat_road_fr;
    CREATE LINKED TABLE stat_road_fr_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT * 
        FROM noisemodelling.stat_road 
        WHERE codedept=lpad(''$codeDep'',3,''0''))');

    CREATE TABLE stat_road_fr AS SELECT * FROM stat_road_fr_link;

    DROP TABLE stat_road_fr_link;


    DROP TABLE IF EXISTS stat_rail_fr_link, stat_rail_fr;
    CREATE LINKED TABLE stat_rail_fr_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT * 
        FROM noisemodelling.stat_rail_geom 
        WHERE codedept=lpad(''$codeDep'',3,''0''))');

    CREATE TABLE stat_rail_fr AS SELECT * FROM stat_rail_fr_link;

    DROP TABLE stat_rail_fr_link;


    DROP TABLE IF EXISTS stat_building_fr_link, stat_building_fr;
    CREATE LINKED TABLE stat_building_fr_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT * 
        FROM noisemodelling.stat_building 
        WHERE codedept=lpad(''$codeDep'',3,''0''))');

    CREATE TABLE stat_building_fr AS SELECT * FROM stat_building_fr_link;

    DROP TABLE stat_building_fr_link;


    DROP TABLE IF EXISTS departement_link;
    CREATE LINKED TABLE departement_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT id, nom_dep, insee_dep, insee_reg 
        FROM noisemodelling.$table_dept 
        WHERE insee_dep=''$codeDep'')');


    -- Update metadata table with end time
    UPDATE metadata SET import_end = NOW();
    """

    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "codeDep": codeDep]


    // print to command window
    logger.info('Manage configuration tables (1/11)')
    def engine = new SimpleTemplateEngine()
    def template = engine.createTemplate(queries_conf).make(binding)

    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage PFAV (2/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_pfav).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage roads (3/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_roads).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage rails (4/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_rails).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage infrastructures (5/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_infra).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage buildings (6/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_buildings).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage screens (7/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_screens).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage buildings screens (8/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_buildings_screens).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage landcover (9/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_landcover).make(binding)
    sql.execute(template.toString())
    progress.endStep()

    logger.info('Manage dem (10/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_dem).make(binding)
    sql.execute(template.toString())
    progress.endStep()

        logger.info('Manage statistics (11/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_stats).make(binding)
    sql.execute(template.toString())
    progress.endStep()



    // ------------------------------------------------------------
    // Rapport part
    def dept_name=sql.firstRow("SELECT nom_dep FROM departement_link;")[0] as String

    def stat_roads_track=sql.firstRow("SELECT NB_TRACK FROM stat_road_fr;")[0] as Integer
    def stat_roads_track_cbsgitt=sql.firstRow("SELECT CBS_GITT_O FROM stat_road_fr;")[0] as Integer

    def nb_roads_track=sql.firstRow("SELECT COUNT(*) FROM ROADS;")[0] as Integer
    def nb_roads=sql.firstRow("SELECT COUNT(DISTINCT ID_ROUTE) FROM ROADS;")[0] as Integer

    def stat_building_nb=sql.firstRow("SELECT nb_building FROM stat_building_fr;")[0] as Integer

    def nb_build=sql.firstRow("SELECT COUNT(*) FROM BUILDINGS;")[0] as Integer
    def nb_build_h0=sql.firstRow("SELECT COUNT(*) FROM BUILDINGS WHERE HEIGHT=0;")[0] as Integer
    def nb_build_hnull=sql.firstRow("SELECT COUNT(*) FROM BUILDINGS WHERE HEIGHT is NULL;")[0] as Integer
    def nb_build_id_erps=sql.firstRow("SELECT COUNT(*) FROM BUILDINGS WHERE id_erps is NOT NULL;")[0] as Integer
    def nb_build_pop=sql.firstRow("SELECT SUM(pop) FROM BUILDINGS;")[0] as Integer


    def stat_rails_track=sql.firstRow("SELECT NB_TRACK FROM stat_rail_fr;")[0] as Integer
    def stat_rails_track_cbsgitt=sql.firstRow("SELECT CBS_GITT_O FROM stat_rail_fr;")[0] as Integer
    def stat_rails_line=sql.firstRow("SELECT NB_LINE FROM stat_rail_fr;")[0] as Integer

    def nb_rail_sections=sql.firstRow("SELECT COUNT(*) FROM RAIL_SECTIONS;")[0] as Integer
    def nb_rail_traffic=sql.firstRow("SELECT COUNT(*) FROM RAIL_TRAFFIC;")[0] as Integer

    def nb_road_screen=sql.firstRow("SELECT COUNT(*) FROM SCREENS WHERE ORIGIN ='road';")[0] as Integer
    def nb_rail_screen=sql.firstRow("SELECT COUNT(*) FROM SCREENS WHERE ORIGIN ='rail';")[0] as Integer

    def nb_land=sql.firstRow("SELECT COUNT(*) FROM LANDCOVER;")[0] as Integer

    def rapport = """
        <!doctype html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width">
            <title data-react-helmet="true">Plamade computation platform</title>
            <link rel="shortcut icon" href="/favicon.ico" type="image/png">
        </head>
        <body>
        <h3>Département n°$codeDep ($dept_name)</h3>
        <hr>
        - Les tables <code>BUILDINGS</code>, <code>ROADS</code>, <code>RAIL_SECTIONS</code>, <code>RAIL_TRAFFIC</code>, <code>SCREENS</code>, 
        <code>LANDCOVER</code>, <code>ZONE</code>, <code>DEM</code> ainsi que celles de configuration ont bien été importées.
        </br></br>
        - Système de projection : EPSG:$srid 
        </br></br>
        - Distance de sélection autour du département et des infrastructures : $buffer m <br/>
        
        <hr>
        <h4>Statistiques</h4>

        À l'échelle du département seul (sans le buffer de $buffer m) et sans filtrage
        <ul>
            <li>Nombre de bâtiments : $stat_building_nb</li>
            <li>Nombre de tronçons routier : $stat_roads_track</li>
            <li>Nombre de tronçons routier utilisables : $stat_roads_track_cbsgitt</li> 
            <li>Nombre de tronçons ferroviaire : $stat_rails_track</li>
            <li>Nombre de tronçons ferroviaire utilisables : $stat_rails_track_cbsgitt</li>
            <li>Nombre de ligne de train : $stat_rails_line</li>       
        </ul>


        Données retenues pour la production de CBS (À l'échelle du département plus les $buffer m de buffer plus les filtres attributaires)

        <h5>Table <code>BUILDINGS</code></h5>
        <ul>
            <li>Nombre de bâtiments : $nb_build</li>
            <li>Nombre avec hauteur = 0 : $nb_build_h0</li>
            <li>Nombre sans hauteur : $nb_build_hnull</li>
            <li>Nombre de bâtiments sensibles : $nb_build_id_erps</li>
            <li>Total population considérée : $nb_build_pop</li>
        </ul>

        <h5>Table <code>ROADS</code></h5>
        <ul>
            <li>Nombre de tronçons: $nb_roads_track</li>
            <li>Nombre de tronçons utilisables pour la carte: $stat_roads_track_cbsgitt</li>
            <li>Nombre de routes: $nb_roads</li>
        </ul>

        <h5>Table <code>RAIL_SECTIONS</code></h5>
        <ul>
            <li>Nombre de tronçons : $nb_rail_sections</li>
        </ul>

        <h5>Table <code>RAIL_TRAFFIC</code></h5>
        <ul>
            <li>Nombre : $nb_rail_traffic</li>
        </ul>

        <h5>Table <code>SCREENS</code></h5>
        <ul>
            <li>Nombre protection acoustique routière: $nb_road_screen</li>
            <li>Nombre protection acoustique ferroviare: $nb_rail_screen</li>
        </ul>

        <h5>Table <code>LANDCOVER</code></h5>
        <ul>
            <li>Nombre de zones : $nb_land</li>
        </ul>

        <br>
        Pour plus de détails, veuillez consulter les tables suivantes :
        <ul>
            <li>BUILDINGS &rarr; <code>STAT_BUILDING_FR</code></li>
            <li>ROADS &rarr; <code>STAT_ROAD_FR</code></li>
            <li>RAIL_SECTIONS &rarr; <code>STAT_RAIL_FR</code></li>
        </ul>
        </body>
        </html>
    """
    
    // Remove non needed tables
    sql.execute("DROP TABLE BUILDINGS, departement_link"); 

    def bindingRapport = ["stat_roads_track" : stat_roads_track, "stat_roads_track_cbsgitt" : stat_roads_track_cbsgitt, "nb_roads_track" : nb_roads_track, "nb_roads" : nb_roads, "nb_build" : nb_build, "nb_build_h0" : nb_build_h0, "nb_build_hnull" : nb_build_hnull, "nb_build_id_erps" : nb_build_id_erps, "nb_build_pop" : nb_build_pop, "nb_rail_sections" : nb_rail_sections, "nb_rail_traffic" : nb_rail_traffic, "nb_land" : nb_land, "buffer": buffer, "codeDep": codeDep, "dept_name" : dept_name, "srid" : srid]
    def templateRapport = engine.createTemplate(rapport).make(bindingRapport)

    // print to WPS Builder
    return templateRapport.toString()

}
