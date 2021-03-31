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

    def databaseUrl = "jdbc:postgresql_h2://plamade.noise-planet.org:5433/plamade_2021_02?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
    //def databaseUrl = input["databaseUrl"] as String
    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String

    def sql = new Sql(connection)

    def queries = """
    DROP TABLE IF EXISTS conf_link, conf;
    CREATE LINKED TABLE conf_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT * FROM noisemodelling.conf)');
    CREATE TABLE conf as select * FROM conf_link;
    DROP TABLE conf_link;
    DROP TABLE IF EXISTS conf_road_link, conf_road;
    CREATE LINKED TABLE conf_road_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT * FROM noisemodelling.conf_road)');
    CREATE TABLE conf_road as select * FROM conf_road_link;
    DROP TABLE conf_road_link;
    DROP TABLE IF EXISTS conf_rail_link, conf_rail;
    CREATE LINKED TABLE conf_rail_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT * FROM noisemodelling.conf_rail)');
    CREATE TABLE conf_rail as select * FROM conf_rail_link;
    DROP TABLE conf_rail_link;
    DROP TABLE IF EXISTS ign_admin_express_dept_l93;
    CREATE LINKED TABLE ign_admin_express_dept_l93 ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT ST_BUFFER(the_geom, $buffer) as the_geom, id, nom_dep_m, nom_dep, insee_dep, insee_reg FROM noisemodelling.ign_admin_express_dept_l93 WHERE insee_dep=''$codeDep'')');
    DROP TABLE IF EXISTS dept_pfav;
    CREATE LINKED TABLE dept_pfav ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT b.insee_dep, a.*   FROM noisemodelling.station_pfav a, noisemodelling.ign_admin_express_dept_l93 b WHERE insee_dep=''$codeDep'' ORDER BY st_distance(a.the_geom, st_centroid(b.the_geom)) LIMIT 1)');
    DROP TABLE IF EXISTS dept;
    CREATE TABLE dept as select * from ign_admin_express_dept_l93;
    ALTER TABLE DEPT ALTER COLUMN ID varchar NOT NULL;
    ALTER TABLE DEPT ADD PRIMARY KEY ( ID);
    CREATE SPATIAL INDEX ON DEPT (THE_GEOM);
    DROP TABLE IF EXISTS ign_admin_express_dept_l93;
    DROP TABLE IF EXISTS dept_meteo;
    CREATE LINKED TABLE dept_meteo ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', '(SELECT codedept, temp_d, temp_e, temp_n, hygro_d, hygro_e, hygro_n, ts_stud, pm_stud FROM echeance4."C_METEO_S_FRANCE" WHERE codedept=''0$codeDep'')');
    DROP TABLE IF EXISTS zone;
    CREATE TABLE zone AS SELECT a.pfav_06_18, a.pfav_18_22, a.pfav_22_06, a.pfav_06_22, b.temp_d, b.temp_e, b.temp_n, b.hygro_d, b.hygro_e, b.hygro_n, b.ts_stud, b.pm_stud FROM dept_pfav a, dept_meteo b;
    DROP TABLE IF EXISTS dept_pfav;
    DROP TABLE IF EXISTS dept_meteo;

	----------------------------------------
    -- Manage rail_sections
    DROP TABLE IF EXISTS rail_sections, rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel;

    CREATE LINKED TABLE rail_sections_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        a."THE_GEOM" as the_geom, 
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
        noisemodelling."N_FERROVIAIRE_TRONCON_L_L93" a,
        echeance4."N_FERROVIAIRE_VITESSE" b,
        (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.ign_admin_express_dept_l93 e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a."CBS_GITT"=''O'' and 
        a."IDTRONCON" = b."IDTRONCON" and
        a."THE_GEOM" && e.the_geom and 
        ST_INTERSECTS(a."THE_GEOM", e.the_geom))');

    CREATE TABLE rail_sections_geom AS SELECT * FROM rail_sections_link;

    CREATE LINKED TABLE rail_tunnel_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTUNNEL" as idtunnel, 
        "IDTRONCON" as idsection 
    FROM 
        echeance4."N_FERROVIAIRE_TUNNEL")');

    CREATE TABLE rail_tunnel AS SELECT * FROM rail_tunnel_link;

    CREATE TABLE rail_sections AS SELECT a.*, b.idtunnel FROM rail_sections_geom a LEFT JOIN rail_tunnel b ON a.idsection = b.idsection;
    CREATE spatial index ON rail_sections (the_geom);

    DROP TABLE rail_sections_link, rail_sections_geom, rail_tunnel_link, rail_tunnel;


	----------------------------------------
    -- Manage buildings
    DROP TABLE IF EXISTS allbuildings_link, buildings_geom, allbuildings_erps_link, buildings_erps, buildings;
    
    CREATE LINKED TABLE allbuildings_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT 
     a.the_geom, 
     a."IDBAT" as id_bat, 
     a."BAT_HAUT" as height,
     b."POP_BAT" as pop
    FROM 
     noisemodelling."C_BATIMENT_S_l93" a,
     echeance4."C_POPULATION" b,
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.ign_admin_express_dept_l93 e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    where
     a.the_geom && c.the_geom and 
     ST_INTERSECTS(a.the_geom, c.the_geom) and 
     a."IDBAT"=b."IDBAT")');
    
    CREATE TABLE buildings_geom as SELECT * from allbuildings_link;
    CREATE SPATIAL INDEX ON buildings_geom(the_geom);
    DELETE FROM buildings_geom B WHERE NOT EXISTS (SELECT 1 FROM rail_sections R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    ALTER TABLE buildings_geom ADD COLUMN pk serial PRIMARY KEY;
    
    -- Get ERPS buildings list
	CREATE LINKED TABLE allbuildings_erps_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
	'(SELECT "IDBAT" as id_bat, "IDERPS" as id_erps FROM echeance4."C_CORRESPOND_BATIMENT_BATIMENTSENSIBLE")');

	CREATE TABLE buildings_erps as SELECT * from allbuildings_erps_link;

	-- Merge both geom and ERPS tables into builings table
	CREATE TABLE buildings as SELECT a.the_geom, a.pk, a.id_bat, a.height, a.pop, b.id_erps FROM buildings_geom a LEFT JOIN buildings_erps b ON a.id_bat = b.id_bat;

	DROP TABLE buildings_geom, buildings_erps, allbuildings_link, allbuildings_erps_link;

	----------------------------------------
	-- Manage Landcover
	DROP TABLE IF EXISTS alllandcover_link, landcover;
    CREATE LINKED TABLE alllandcover_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', '(SELECT 
     a.the_geom, 
     a.idnatsol as pk, 
     a.natsol_lib as clc_lib,
     a.natsol_cno as g
    FROM 
     noisemodelling."C_NATURESOL_S_L93" a,
     (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.ign_admin_express_dept_l93 e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    WHERE
     a.the_geom && c.the_geom and 
     ST_INTERSECTS(a.the_geom, c.the_geom) and 
     a.natsol_cno > 0)');
    
    CREATE TABLE landcover as select * from alllandcover_link;
    CREATE SPATIAL INDEX ON landcover(the_geom);
    DELETE FROM landcover B WHERE NOT EXISTS (SELECT 1 FROM rail_sections R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    DROP TABLE alllandcover_link;

    DROP TABLE DEPT;
    """


    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "codeDep": codeDep]

    def engine = new SimpleTemplateEngine()
    def template = engine.createTemplate(queries).make(binding)

    sql.execute(template.toString())
    // print to WPS Builder
    return "Table BUILDINGS, RAIL_SECTIONS, LANDCOVER, ZONE and CONF imported"

}

