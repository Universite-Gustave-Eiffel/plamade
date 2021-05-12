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
 */

package org.noise_planet.noisemodelling.wps.Plamade

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.text.SimpleTemplateEngine
import org.geotools.jdbc.JDBCDataStore
import org.locationtech.jts.geom.Point
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
        //databaseUrl : [
        //        name       : 'PostGIS host',
        //        title      : 'Url of the PostGIS database',
        //        description: 'Plamade server url in the form of jdbc:postgresql_h2://ip_adress:port/db_name',
        //        type       : String.class
        //],
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

    // print to command window
    logger.info('Start linking with PostGIS')

    // Get provided parameters
    String codeDep = input["inseeDepartment"] as String
    Integer buffer = 1000
    if ('fetchDistance' in input) {
        buffer = input["fetchDistance"] as Integer
    }

    def databaseUrl = "jdbc:postgresql_h2://plamade.noise-planet.org:5433/plamade_2021_05_03?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
    //def databaseUrl = input["databaseUrl"] as String
    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String


    // Declare table variables depending on the department and the projection system
    def srid = "2154"
    def table_station = "station_pfav_2154"
    def table_dept = "ign_admin_express_dept_l93"
    def table_route = "N_ROUTIER_TRONCON_L_2154" 
    def table_rail = "N_FERROVIAIRE_TRONCON_L_2154"
    def table_bati = "C_BATIMENT_S_2154"
    def table_route_protect = "N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154"
    def table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154"
    def table_land = "C_NATURESOL_S_2154"

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
    }


    def sql = new Sql(connection)

    def queries = """
    
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
    
    -- Manage department table
--    DROP TABLE IF EXISTS departement_link;
--    CREATE LINKED TABLE departement_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
--        '(SELECT ST_BUFFER(the_geom, $buffer) as the_geom, id, nom_dep, insee_dep, insee_reg 
--        FROM noisemodelling.$table_dept 
--        WHERE insee_dep=''$codeDep'')');
    
    ----------------------------------
    -- Manage pfav and meteo tables

    DROP TABLE IF EXISTS dept_pfav;
    CREATE LINKED TABLE dept_pfav ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT b.insee_dep, a.* 
        FROM noisemodelling.$table_station a, noisemodelling.$table_dept b 
        WHERE insee_dep=''$codeDep'' ORDER BY st_distance(a.the_geom, st_centroid(b.the_geom)) LIMIT 1)');
    
--    DROP TABLE IF EXISTS dept;
--    CREATE TABLE dept as select * from departement_link;
--    ALTER TABLE DEPT ALTER COLUMN ID varchar NOT NULL;
--    ALTER TABLE DEPT ADD PRIMARY KEY ( ID);
--    CREATE SPATIAL INDEX ON DEPT (THE_GEOM);
--    DROP TABLE IF EXISTS departement_link;
    
    DROP TABLE IF EXISTS dept_meteo;
    CREATE LINKED TABLE dept_meteo ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT "CODEDEPT" as codedept, temp_d, temp_e, temp_n, hygro_d, hygro_e, hygro_n, ts_stud, pm_stud 
        FROM echeance4."C_METEO_S" 
        WHERE "CODEDEPT"=lpad(''$codeDep'',3,''0''))');
    
    DROP TABLE IF EXISTS zone;
    CREATE TABLE zone AS SELECT 
        a.pfav_06_18, a.pfav_18_22, a.pfav_22_06, a.pfav_06_22, 
        b.temp_d, b.temp_e, b.temp_n, b.hygro_d, b.hygro_e, b.hygro_n, b.ts_stud, b.pm_stud 
        FROM dept_pfav a, dept_meteo b;
    
    DROP TABLE IF EXISTS dept_pfav,dept_meteo;

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
     c."VITESSEPL" as hgv_spd_e, c."VITESSEPL" as hgv_spd_n, c."VITESSEVL" as wav_spd_d, c."VITESSEVL" as wav_spd_e, c."VITESSEVL" as wav_spd_n, 
     c."VITESSEVL" as wbv_spd_d, c."VITESSEVL" as wbv_spd_e, c."VITESSEVL" as wbv_spd_n,
     d."REVETEMENT" as revetement,
     d."GRANULO" as granulo,
     d."CLASSACOU" as classacou,
     a."NB_VOIES" as ntrack,
     a."LARGEUR" as width,
     a."ZDEB" as z_start,
     ST_Z(ST_StartPoint(a.the_geom)) as z_debut, 
     a."ZFIN" as z_end,
     ROUND((a."ZFIN"-a."ZDEB")/ ST_LENGTH(a.the_geom)*100) as slope,
     a."SENS" as sens,
     (CASE  WHEN a."SENS" = ''01'' THEN ''01'' 
       WHEN a."SENS" = ''02'' THEN ''02'' 
       ELSE ''03''
      END) as way
    FROM 
     noisemodelling."$table_route" a,
     echeance4."N_ROUTIER_TRAFIC" b,
     echeance4."N_ROUTIER_VITESSE" c,
     echeance4."N_ROUTIER_REVETEMENT" d, 
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e,
     echeance4."N_ROUTIER_ROUTE" f 
    WHERE 
     a."CBS_GITT"=''O'' and
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


    ----------------------------------
    -- Manage rail_sections

    DROP TABLE IF EXISTS rail_sections, rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel, rail_trafic_link, rail_trafic;

    CREATE LINKED TABLE rail_sections_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        a.the_geom, 
        a."IDTRONCON" as idsection,
        a."NB_VOIES_1" as ntrack, 
        a."IDLIGNE" as idline, 
        a."NUMLIGNE" as numline, 
        a."VMAXINFRA" as speedtrack, 
        a."BASEVOIE" as tracktransfer, 
        a."RUGOSITE" as railroughness, 
        a."JOINTRAIL" as impactnoise, 
        a."COURBURE" as curvature, 
        a."BASEVOIE" as bridgetransfer, 
        b."VITESSE" as speedcommercial  
    FROM 
        noisemodelling."$table_rail" a,
        echeance4."N_FERROVIAIRE_VITESSE" b,
        (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a."CBS_GITT"=''O'' and 
        a."IDTRONCON" = b."IDTRONCON" and
        a.the_geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE rail_sections_geom AS SELECT * FROM rail_sections_link;

    CREATE LINKED TABLE rail_tunnel_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTUNNEL" as idtunnel, 
        "IDTRONCON" as idsection 
    FROM 
        echeance4."N_FERROVIAIRE_TUNNEL")');

    CREATE TABLE rail_tunnel AS SELECT * FROM rail_tunnel_link;

    CREATE TABLE rail_sections AS SELECT a.*, b.idtunnel FROM rail_sections_geom a LEFT JOIN rail_tunnel b ON a.idsection = b.idsection;
    CREATE SPATIAL INDEX rail_sections_geom_idx ON rail_sections (the_geom);
    CREATE INDEX ON rail_sections (idsection);


    -- Rail_trafic
    CREATE LINKED TABLE rail_trafic_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTRAFIC" as idtrafic,
        "IDTRONCON" as idsection,
        "VMAX" as speedvehicule,
        "ENGMOTEUR" as typetrain,
        "TDIURNE" as tday,
        "TSOIR" as tevening,
        "TNUIT" as tnight 
    FROM 
        echeance4."N_FERROVIAIRE_TRAFIC")');

    CREATE TABLE rail_trafic AS SELECT a.* FROM rail_trafic_link a, rail_sections b WHERE a.idsection=b.idsection;


    DROP TABLE rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel, rail_trafic_link;


    ----------------------------------
    -- Generate infrastructure table (merge of roads and rails)

    DROP TABLE IF EXISTS infra;
    CREATE TABLE infra AS SELECT the_geom FROM roads UNION SELECT the_geom FROM rail_sections;
    CREATE SPATIAL INDEX infra_geom_idx ON infra (the_geom);


	----------------------------------
    -- Manage buildings

    DROP TABLE IF EXISTS allbuildings_link, buildings_geom, allbuildings_erps_link, buildings_erps, buildings;
    
    CREATE LINKED TABLE allbuildings_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT 
     a.the_geom, 
     a."IDBAT" as id_bat, 
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
    DELETE FROM buildings_geom B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
        
    -- Get ERPS buildings list
	CREATE LINKED TABLE allbuildings_erps_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
	'(SELECT "IDBAT" as id_bat, "IDERPS" as id_erps FROM echeance4."C_CORRESPOND_BATIMENT_BATIMENTSENSIBLE")');

	CREATE TABLE buildings_erps as SELECT * from allbuildings_erps_link;

	-- Merge both geom and ERPS tables into builings table
	CREATE TABLE buildings as SELECT a.the_geom, a.id_bat, a.height, a.pop, b.id_erps FROM buildings_geom a LEFT JOIN buildings_erps b ON a.id_bat = b.id_bat;
    ALTER TABLE buildings ADD COLUMN pk serial PRIMARY KEY;
    ALTER TABLE buildings ADD COLUMN g float DEFAULT 0.1;
    ALTER TABLE buildings ADD COLUMN origin varchar DEFAULT 'building';
    
	DROP TABLE buildings_geom, buildings_erps, allbuildings_link, allbuildings_erps_link;


    ----------------------------------
    -- Manage acoustic screenss

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

    ALTER TABLE screens ADD COLUMN g float DEFAULT 0;
    UPDATE screens SET g = 0.7 WHERE propriete = '01';
    UPDATE screens SET g = 0.7 WHERE (propriete = '00' or propriete = '99') AND (materiau1 = '01' or materiau1 = '04' or materiau1 = '06');

    ALTER TABLE screens ADD COLUMN pop integer DEFAULT 0;
    ALTER TABLE screens ADD COLUMN id_erps integer DEFAULT 0;
    ALTER TABLE screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON screens(the_geom);

    DROP TABLE road_screens_link, road_screens, rail_screens_link, rail_screens;


    ----------------------------------
    -- Merge buildings and screenss

    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buildings_screens;

    CREATE TABLE tmp_relation_screen_building AS SELECT b.pk as pk_building, s.pk as pk_screen 
        FROM buildings b, screens s 
        WHERE b.the_geom && s.the_geom AND ST_Distance(b.the_geom, s.the_geom) <= 0.5;

    -- For intersecting screens, remove parts closer than distance_truncate_screens
    CREATE TABLE tmp_screen_truncated AS SELECT pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 0.5)) the_geom, s.id_bat, s.height, s.pop, s.id_erps, s.g, s.origin 
        FROM tmp_relation_screen_building r, buildings b, screens s 
        WHERE pk_building = b.pk AND pk_screen = s.pk 
        GROUP BY pk_screen, s.id_bat, s.height, s.pop, s.id_erps, s.g, s.origin;

    -- Merge untruncated screens and truncated screens
    CREATE TABLE tmp_screens AS 
        SELECT the_geom, pk, id_bat, height, pop, id_erps, g, origin FROM screens WHERE pk not in (SELECT pk_screen FROM tmp_screen_truncated) UNION ALL 
        SELECT the_geom, pk_screen as pk, id_bat, height, pop, id_erps, g, origin FROM tmp_screen_truncated;

    -- Convert linestring screens to polygons with buffer function
    CREATE TABLE tmp_buffered_screens AS SELECT ST_BUFFER(sc.the_geom, 0.1, 'join=mitre endcap=flat') as the_geom, pk, id_bat, height, pop, id_erps, g, origin 
        FROM tmp_screens sc;
    
    -- Merge buildings and buffered screens
    CREATE TABLE buildings_screens as 
        SELECT the_geom, id_bat, height, pop, id_erps, g, origin FROM tmp_buffered_screens sc UNION ALL 
        SELECT the_geom, id_bat, height, pop, id_erps, g, origin FROM buildings;

    ALTER TABLE buildings_screens ADD COLUMN pk serial PRIMARY KEY;
    UPDATE buildings_screens SET the_geom  = ST_SetSRID(the_geom, '$srid');
    CREATE SPATIAL INDEX ON buildings_screens(the_geom);


    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buffered_screens;
    

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

    DROP TABLE PVMT;
    --DROP INFRA;

    ----------------------------------
    -- Manage statitics
    
    DROP TABLE IF EXISTS stat_road_fr_link, stat_road_fr;
    CREATE LINKED TABLE stat_road_fr_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
        '(SELECT * 
        FROM noisemodelling.stat_road 
        WHERE codedept=lpad(''$codeDep'',3,''0''))');

    CREATE TABLE stat_road_fr AS SELECT * FROM stat_road_fr_link;

    DROP TABLE stat_road_fr_link;

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

    """

    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "codeDep": codeDep]

    def engine = new SimpleTemplateEngine()
    def template = engine.createTemplate(queries).make(binding)

    sql.execute(template.toString())

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

    def nb_rail_sections=sql.firstRow("SELECT COUNT(*) FROM RAIL_SECTIONS;")[0] as Integer
    def nb_rail_trafic=sql.firstRow("SELECT COUNT(*) FROM RAIL_TRAFIC;")[0] as Integer

    def nb_land=sql.firstRow("SELECT COUNT(*) FROM LANDCOVER;")[0] as Integer

    def rapport = """
        <h3>Département n°$codeDep ($dept_name)</h3>
        <hr>
        - Les tables <code>BUILDINGS</code>, <code>ROADS</code>, <code>RAIL_SECTIONS</code>, <code>RAIL_TRAFIC</code>, 
        <code>LANDCOVER</code>, <code>ZONE</code> et <code>CONF</code> ont bien été importées.
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

        <h5>Table <code>RAIL_TRAFIC</code></h5>
        <ul>
            <li>Nombre : $nb_rail_trafic</li>
        </ul>

        <h5>Table <code>LANDCOVER</code></h5>
        <ul>
            <li>Nombre de zones : $nb_land</li>
        </ul>

        <br>
        Pour plus de détails, veuillez consulter les tables suivantes :
        <ul>
            <li>BUILDINGS &rarr; <code>STAT_BUILDING</code></li>
            <li>ROADS &rarr; <code>STAT_ROAD</code></li>
            <li>RAIL_SECTIONS &rarr; <code></code></li>
            <li>RAIL_TRAFIC &rarr; <code></code></li>
            <li>LANDCOVER &rarr; <code>STAT_LANDCOVER</code></li>
        </ul>

    """
    def bindingRapport = ["stat_roads_track" : stat_roads_track, "stat_roads_track_cbsgitt" : stat_roads_track_cbsgitt, "nb_roads_track" : nb_roads_track, "nb_roads" : nb_roads, "nb_build" : nb_build, "nb_build_h0" : nb_build_h0, "nb_build_hnull" : nb_build_hnull, "nb_build_id_erps" : nb_build_id_erps, "nb_build_pop" : nb_build_pop, "nb_rail_sections" : nb_rail_sections, "nb_rail_trafic" : nb_rail_trafic, "nb_land" : nb_land, "buffer": buffer, "codeDep": codeDep, "dept_name" : dept_name, "srid" : srid]
    def templateRapport = engine.createTemplate(rapport).make(bindingRapport)

    // print to WPS Builder
    return templateRapport.toString()
}