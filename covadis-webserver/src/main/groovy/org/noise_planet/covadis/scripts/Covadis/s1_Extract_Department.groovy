/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team FROM the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/cbs_uge_input.html>
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

package org.noise_planet.covadis.scripts.Covadis

import groovy.sql.Sql
import groovy.text.SimpleTemplateEngine
import groovy.transform.CompileStatic
import org.h2.util.ScriptReader
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
//import org.noise_planet.cbs_uge_input.pathfinder.RootProgressVisitor
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

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

@CompileStatic
static def parseScript(String sqlInstructions, Sql sql, ProgressVisitor progressVisitor, Logger logger) {
    Reader reader = null
    ByteArrayInputStream s = new ByteArrayInputStream(sqlInstructions.getBytes())
    InputStream is = s
    List<String> statementList = new LinkedList<>()
    try {
        reader  = new InputStreamReader(is)
        ScriptReader scriptReader = new ScriptReader(reader)
        scriptReader.setSkipRemarks(true)
        String statement = scriptReader.readStatement()
        while (statement != null && !statement.trim().isEmpty()) {
            statementList.add(statement)
            statement = scriptReader.readStatement()
        }
    } finally {
        reader.close()
    }
    int idStatement = 0
    final int nbStatements = statementList.size()
    ProgressVisitor evalProgress = progressVisitor.subProcess(nbStatements)
    for(String statement : statementList) {
        logger.info(String.format(Locale.ROOT, "%d/%d %s", (idStatement++) + 1, nbStatements, statement.trim()))
        sql.execute(statement)
        evalProgress.endStep()
        if(evalProgress.isCanceled()) {
            throw new SQLException("Canceled by user")
        }
    }
}

def exec(Connection connection, input) {


    //------------------------------------------------------
    // Clean the database before starting the importation

    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

    // Build the result string with every tables
    StringBuilder sb = new StringBuilder()

    // Get every table names
    List<String> tables = JDBCUtilities.getTableNames(connection, null, "PUBLIC", "%", null)

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

    ProgressVisitor progress = progressVisitor.subProcess(2)
    // print to command window
    logger.info('Start linking with PostGIS')

    // Get provided parameters
    String codeDep = input["inseeDepartment"] as String

    String codeDepFormat = codeDep.size() == 2 ? "0$codeDep" : codeDep


    Integer buffer = 1000
    if ('fetchDistance' in input) {
        buffer = input["fetchDistance"] as Integer
    }

//ogr2ogr -f "PostgreSQL" PG:"host=postgresql-645ae675-o823ba0cf.database.cloud.ovh.net port=20184 dbname=cbs2027_bdtmja user=gwendallpuge password=gwendallpuge2482"     



    //?ssl=true&sslmode=disable
    String databaseUrl="jdbc:postgresql_h2://postgresql-645ae675-o823ba0cf.database.cloud.ovh.net:20184/cbs2027_bdtmja"


    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String

    // Declare table variables depending on the department and the projection system
    def srid = "2154"
    def table_station = "station_pfav_2154"
    def table_dept = "nm_departement_2154"
    def table_route = "n_routier_troncon_l_aura_fake_69_fixed"
    def table_rail = "N_FERROVIAIRE_TRONCON_L_2154"
    def table_bati = "c_batiment_s_aura"
    def table_route_protect = "n_route_protection_acoustique_france"
    def table_rail_protect = "N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154"
    def table_agglo = "agglo_2154"
    def table_land = "c_naturesol_hexagone"
    def table_infra = "infra_2154"
    def table_bd_topo_route = "t_route_metro_corse"
    def table_bd_topo_rail = "t_fer_metro_corse"
    def table_bd_topo_oro = "oro_metro_corse"
    def table_bd_topo_hydro = "t_hydro_metro_corse"




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

    if(codeDep=='971') {
        table_bd_topo_route = 't_route_guadeloupe'
        table_bd_topo_rail = 't_fer_guadeloupe'
        table_bd_topo_oro = 'oro_guadeloupe'
        table_bd_topo_hydro = 't_hydro_guadeloupe'
    } else if(codeDep == '972') {
        table_bd_topo_route = 't_route_martinique'
        table_bd_topo_rail = 't_fer_martinique'
        table_bd_topo_oro = 'oro_martinique'
        table_bd_topo_hydro = 't_hydro_martinique'
    } else if(codeDep == '973') {
        table_bd_topo_route = 't_route_guyane'
        table_bd_topo_rail = 't_fer_guyane'
        table_bd_topo_oro = 'oro_guyane'
        table_bd_topo_hydro = 't_hydro_guyane'
    } else if(codeDep == '974') {
        table_bd_topo_route = 't_route_reunion'
        table_bd_topo_rail = 't_fer_reunion'
        table_bd_topo_oro = 'oro_reunion'
        table_bd_topo_hydro = 't_hydro_reunion'
    } else if(codeDep == '976') {
        table_bd_topo_route = 't_route_mayotte'
        table_bd_topo_rail = 't_fer_mayotte'
        table_bd_topo_oro = 'oro_mayotte'
        table_bd_topo_hydro = 't_hydro_mayotte'
    }

    def sql = new Sql(connection)

    def queries_conf = """
    ----------------------------------
    -- Manage metadata tables

    DROP TABLE IF EXISTS nuts_link, metadata;
    CREATE LINKED TABLE nuts_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT code_2021 as nuts, ratio_pop_log FROM cbs_uge_input.nm_nuts WHERE code_dept=''$codeDepFormat'')');

    CREATE TABLE metadata (code_dept varchar, nuts varchar, ratio_pop_log double, srid integer, import_start timestamp, import_end timestamp, 
        grid_conf integer, grid_start timestamp, grid_end timestamp, 
        emi_conf integer, emi_start timestamp, emi_end timestamp, 
        road_conf integer, road_start timestamp, road_end timestamp, 
        rail_conf integer, rail_start timestamp, rail_end timestamp);

    INSERT INTO metadata (code_dept, nuts, ratio_pop_log, srid, import_start) VALUES ('$codeDep', (SELECT "nuts" FROM nuts_link), (SELECT "ratio_pop_log" FROM nuts_link), $srid, NOW());
    
    DROP TABLE nuts_link;

    ----------------------------------
    -- Manage configuration tables

    -- CONF
    DROP TABLE IF EXISTS conf_link, conf;
    CREATE LINKED TABLE conf_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT * FROM cbs_uge_input.nm_conf)');
    CREATE TABLE conf as select * FROM conf_link;
    DROP TABLE conf_link;
    
    -- CONF_ROAD
    DROP TABLE IF EXISTS conf_road_link, conf_road;
    CREATE LINKED TABLE conf_road_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT * FROM cbs_uge_input.nm_conf_road)');
    CREATE TABLE conf_road as select * FROM conf_road_link;
    DROP TABLE conf_road_link;
    
    -- CONF_RAIL
    DROP TABLE IF EXISTS conf_rail_link, conf_rail;
    CREATE LINKED TABLE conf_rail_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT * FROM cbs_uge_input.nm_conf_rail)');
    CREATE TABLE conf_rail as select * FROM conf_rail_link;
    DROP TABLE conf_rail_link;

    -- PLATEFORME
    DROP TABLE IF EXISTS plateform_link, plateform;
    CREATE LINKED TABLE plateform_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT * FROM cbs_uge_input.nm_platform)');
    CREATE TABLE plateform as select * FROM plateform_link;
    DROP TABLE plateform_link;

    """


    def queries_roads = """
    ----------------------------------
    -- Manage roads

    DROP TABLE IF EXISTS roads_link, roads, pvmt_link;

    CREATE LINKED TABLE roads_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
    '(SELECT  st_translate(st_force3dz(a.geom3d), 0, 0, 0.05) as "THE_GEOM", 
     a.idtroncon as "ID_TRONCON", a.idroute as "ID_ROUTE", f.nomroute as "NOM_ROUTE",
     b.tmhvld as "LV_D", b.tmhvls as "LV_E", b.tmhvln as "LV_N",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_D",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_E",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_N",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b."pcenthpl"/b.pcentpl ELSE 0 END) as "HGV_D",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b."pcenthpl"/b.pcentpl ELSE 0 END) as "HGV_E",
     (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b."pcenthpl"/b.pcentpl ELSE 0 END) as "HGV_N",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rd * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_D",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rs * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_E",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rn * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_N",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rd * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_D",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rs * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_E",
     (CASE  WHEN b.pcent2r > 0 THEN b.tmjh2rn * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_N",
     c.vitessevl as "LV_SPD_D", c.vitessevl as "LV_SPD_E", c.vitessevl as "LV_SPD_N", 
     c."vitessepl" as "MV_SPD_D",c."vitessepl" as "MV_SPD_E", c."vitessepl" as "MV_SPD_N", 
     c."vitessepl" as "HGV_SPD_D", c."vitessepl" as "HGV_SPD_E", c."vitessepl" as "HGV_SPD_N", 
     c."vitesse4a" as "WAV_SPD_D", c."vitesse4a" as "WAV_SPD_E", c."vitesse4a" as "WAV_SPD_N", 
     c."vitesse4b" as "WBV_SPD_D", c."vitesse4b" as "WBV_SPD_E", c."vitesse4b" as "WBV_SPD_N",
     d.revetement as "REVETEMENT",
     d.granulo as "GRANULO",
     d.classacou as "CLASSACOU",
     a.nb_voies as "NTRACK",
     a.largeur as "WIDTH",
     a.zdeb as "Z_START",
     ST_Z(ST_StartPoint(a.geom3d)) as "Z_DEBUT", 
     a.zfin as "Z_END",
     ROUND((a.zfin-a.zdeb)/ ST_LENGTH(a.geom3d)*100) as "SLOPE",
     (CASE  WHEN a.sens = ''01'' THEN ''01'' 
       WHEN a.sens = ''02'' THEN ''02'' 
       ELSE ''03''
      END) as "WAY",
     f.uueid as "UUEID"--,
     --a."agglo" as agglo 
    FROM 
     cbs_uge_input."$table_route" a,
     cbs_uge_input.n_routier_trafic_aura_fake b,
     cbs_uge_input.n_routier_vitesse_aura_fake c,
     cbs_uge_input.n_routier_revetement_aura_fake d,
     cbs_uge_input.n_routier_route_aura_fake f 
    WHERE 
     ST_LENGTH(a.geom3d)>0 and
     a.codedept = lpad(''$codeDep'',3,''0'') and
     a.idtroncon=b.idtroncon and
     a.idtroncon=c.idtroncon and
     a.idtroncon=d.idtroncon and 
     a.idroute=f.idroute)');
    
    CREATE TABLE ROADS AS SELECT * FROM roads_link;
    DROP TABLE roads_link;
    ALTER TABLE ROADS ADD COLUMN pvmt varchar(4);
    CREATE LINKED TABLE pvmt_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 'nm_pvmt');
    DROP TABLE IF EXISTS PVMT;
    CREATE TABLE PVMT as select "revetement" as revetement, "granulo" as granulo, "classacou" as classacou, "total" as total, "pvmt" as pvmt FROM pvmt_link;
    DROP TABLE pvmt_link;
    CREATE INDEX ON PVMT (revetement);
    CREATE INDEX ON PVMT (granulo);
    CREATE INDEX ON PVMT (classacou);
    UPDATE ROADS b SET pvmt = (select a.pvmt FROM pvmt a WHERE a.revetement=b.revetement AND a.granulo=b.granulo AND a.classacou=b.classacou);
    ALTER TABLE roads ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON ROADS (the_geom);
    CREATE INDEX ON ROADS(uueid);
    """

/** queries_rails
 * 
 * 
*/



    def queries_infra = """
    ----------------------------------
    -- Generate infrastructure table (merge of roads and rails)

    DROP TABLE IF EXISTS infra;
    CREATE TABLE infra AS SELECT the_geom FROM roads;
    -- En attendant d'avoir les voies ferrees
    -- CREATE TABLE infra AS SELECT the_geom FROM roads UNION ALL SELECT the_geom FROM rail_sections;
    CREATE SPATIAL INDEX infra_geom_idx ON infra (the_geom);

    """


    def queries_buildings = """
    ----------------------------------
    -- Manage buildings

    DROP TABLE IF EXISTS allbuildings_link, buildings_geom, allbuildings_erps_link, allbuildings_erps, allbuildings_erps_natur_link, allbuildings_erps_natur, buildings_erps_natur, buildings;
    
    CREATE LINKED TABLE allbuildings_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', '(SELECT 
     a.geom as "THE_GEOM", 
     a.idbat as "ID_BAT", 
     a.bat_idtopo as "BAT_IDTOPO",
     a.hauteur as "HEIGHT",
     b.pop_bat as "POP",
     --a.agglo as "AGGLO" 
     null as "AGGLO"
    FROM 
     cbs_uge_input."$table_bati" a,
     cbs_uge_input."c_population_aura" b,
     (select ST_BUFFER(the_geom, $buffer) the_geom FROM cbs_uge_input.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    where
     a.geom && c.the_geom and 
     ST_INTERSECTS(a.geom, c.the_geom) and 
     a.idbat=b.idbat)');
    
    CREATE TABLE buildings_geom as SELECT * FROM allbuildings_link;
    CREATE SPATIAL INDEX ON buildings_geom(the_geom);
    CREATE INDEX ON buildings_geom(id_bat);
    DELETE FROM buildings_geom B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
        
    -- Get ERPS buildings list
    CREATE LINKED TABLE allbuildings_erps_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
    '(SELECT idbat as "ID_BAT", iderps as "ID_ERPS" FROM cbs_uge_input.c_correspond_batiment_batimentsensible_aura_fake)');

    -- Get ERPS nature
    CREATE LINKED TABLE allbuildings_erps_natur_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
    '(SELECT iderps as "ID_ERPS", erps_nature as "ERPS_NATUR" FROM cbs_uge_input.c_batimentsensible_aura_fake)');

    -- Save linked tables to be able to create indexes
    CREATE TABLE allbuildings_erps AS SELECT * FROM allbuildings_erps_link;
    CREATE TABLE allbuildings_erps_natur AS SELECT * FROM allbuildings_erps_natur_link;
    CREATE INDEX ON allbuildings_erps(id_erps);
    CREATE INDEX ON allbuildings_erps_natur(id_erps);

    -- Merge both ERPS informations
    CREATE TABLE buildings_erps_natur as SELECT a.*, b.erps_natur FROM allbuildings_erps a, allbuildings_erps_natur b WHERE a.id_erps = b.id_erps;
    CREATE INDEX ON buildings_erps_natur(id_bat);

    -- Merge both geom and ERPS tables into builings table
    CREATE TABLE buildings AS SELECT the_geom, id_bat, bat_idtopo, height, pop, agglo FROM buildings_geom;
    ALTER TABLE buildings ADD COLUMN pk serial PRIMARY KEY;
    ALTER TABLE buildings ADD COLUMN g float DEFAULT 0.1;
    ALTER TABLE buildings ADD COLUMN origin varchar DEFAULT 'building';
    CREATE SPATIAL INDEX ON buildings(the_geom);
    
    
    -- Reduce ERPS list with existing buildings
    CREATE TABLE buildings_erps AS SELECT a.* FROM buildings_erps_natur a, buildings b WHERE a.id_bat=b.id_bat;
 
    DROP TABLE buildings_geom, buildings_erps_natur, allbuildings_link, allbuildings_erps_link, allbuildings_erps, allbuildings_erps_natur_link, allbuildings_erps_natur;

    """


    def queries_screens = """
    ----------------------------------
    -- Manage acoustic screens

    -- For roads

    DROP TABLE IF EXISTS road_screens_link, road_screens;

    CREATE LINKED TABLE road_screens_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT
        a.geom as "THE_GEOM",
        a.idprotacou as "ID_BAT",
        a.hauteur as "HEIGHT",
        a.propriete as "PROPRIETE",
        a.materiau1 as "MATERIAU1"
    FROM 
        cbs_uge_input."$table_route_protect" a,
        (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a.geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE road_screens AS SELECT * FROM road_screens_link;
    ALTER TABLE road_screens ADD COLUMN origin varchar DEFAULT 'road';

    DELETE FROM road_screens B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);

    -- For rail

    DROP TABLE IF EXISTS rail_screens_link, rail_screens;

    CREATE LINKED TABLE rail_screens_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT
        a.the_geom,
        a.idprotacou as "ID_BAT",
        a.hauteur as "HEIGHT",
        a.propriete as "PROPRIETE",
        a.materiau1 as "MATERIAU1"        
    FROM 
        noisemodelling."$table_rail_protect" a,
        (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e
    WHERE
        a.the_geom && e.the_geom and 
        ST_INTERSECTS(a.the_geom, e.the_geom))');

    CREATE TABLE rail_screens AS SELECT * FROM rail_screens_link;
    ALTER TABLE rail_screens ADD COLUMN origin varchar DEFAULT 'rail';

    DELETE FROM rail_screens B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);


    -- Merge both screens tables
    DROP TABLE IF EXISTS screens;
    CREATE TABLE screens AS SELECT * FROM road_screens UNION ALL SELECT * FROM rail_screens;

    ALTER TABLE screens ADD COLUMN bat_idtopo varchar;
    ALTER TABLE screens ADD COLUMN g float DEFAULT 0;
    UPDATE screens SET g = 0.7 WHERE propriete = '01';
    UPDATE screens SET g = 0.7 WHERE (propriete = '00' or propriete = '99') AND (materiau1 = '01' or materiau1 = '04' or materiau1 = '06');

    ALTER TABLE screens ADD COLUMN pop integer DEFAULT 0;
    ALTER TABLE screens ADD COLUMN agglo boolean DEFAULT false;
    ALTER TABLE screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON screens(the_geom);

    DROP TABLE road_screens_link, road_screens, rail_screens_link, rail_screens;

    """



    def queries_buildings_screens = """
    ----------------------------------
    -- Merge buildings and screens

    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buildings_screens;

    CREATE TABLE tmp_relation_screen_building AS SELECT b.pk as pk_building, s.pk as pk_screen 
        FROM buildings b, screens s 
        WHERE b.the_geom && s.the_geom AND ST_Distance(b.the_geom, s.the_geom) <= 0.5;

    -- For intersecting screens, remove parts closer than distance_truncate_screens
    CREATE TABLE tmp_screen_truncated AS SELECT pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 0.5)) the_geom, s.id_bat, s.bat_idtopo, s.height, s.pop, s.agglo, s.g, s.origin 
        FROM tmp_relation_screen_building r, buildings b, screens s 
        WHERE pk_building = b.pk AND pk_screen = s.pk 
        GROUP BY pk_screen, s.id_bat, s.bat_idtopo, s.height, s.pop, s.g, s.origin;

    -- Merge untruncated screens and truncated screens
    CREATE TABLE tmp_screens AS 
        SELECT the_geom, pk, id_bat, bat_idtopo, height, pop, agglo, g, origin FROM screens WHERE pk not in (SELECT pk_screen FROM tmp_screen_truncated) UNION ALL 
        SELECT the_geom, pk_screen as pk, id_bat, bat_idtopo, height, pop, agglo, g, origin FROM tmp_screen_truncated;

    -- Convert linestring screens to polygons with buffer function
    CREATE TABLE tmp_buffered_screens AS SELECT ST_SETSRID(ST_BUFFER(sc.the_geom, 0.1, 'join=mitre endcap=flat'), ST_SRID(sc.the_geom)) as the_geom, pk, id_bat, bat_idtopo, height, pop, agglo, g, origin 
        FROM tmp_screens sc;

    -- Merge buildings and buffered screens
    CREATE TABLE buildings_screens as 
        SELECT the_geom, id_bat, bat_idtopo, height, pop, agglo, g, origin FROM tmp_buffered_screens sc UNION ALL 
        SELECT the_geom, id_bat, bat_idtopo, height, pop, agglo, g, origin FROM buildings;

    -- Add a column to know if the building has 1 or n ERPS inside
    ALTER TABLE buildings_screens ADD COLUMN erps boolean default false;
    UPDATE buildings_screens SET erps = true WHERE id_bat IN (SELECT DISTINCT id_bat FROM buildings_erps);
    ALTER TABLE buildings_screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON buildings_screens(the_geom);
    
    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buffered_screens;

    """


    def queries_landcover = """
    ----------------------------------
    -- Manage Landcover
    
    DROP TABLE IF EXISTS alllandcover_link, LANDCOVER;
    CREATE LINKED TABLE alllandcover_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', '(SELECT 
     a.geom as "THE_GEOM", 
     a.idnatsol as "PK", 
     a.natsol_lib as "CLC_LIB",
     a.natsol_cno as "G"
    FROM 
     cbs_uge_input."$table_land" a,
     (select ST_BUFFER(the_geom, $buffer) the_geom FROM cbs_uge_input.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) c 
    WHERE
     a."geom" && c.the_geom and 
     ST_INTERSECTS(a.geom, c.the_geom) and 
     a.natsol_cno > 0)');
    
    CREATE TABLE LANDCOVER as select * FROM alllandcover_link;
    CREATE SPATIAL INDEX ON LANDCOVER(the_geom);
    DELETE FROM LANDCOVER B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.the_geom, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    DROP TABLE alllandcover_link;

    """


    def queries_landcover_rail = """
    

    """


    def queries_dem = """
     ----------------------------------
    -- Import data and filtering within 1000m around infra

    ------------
    -- Import DEM FROM BD Alti

    DROP TABLE IF EXISTS bdalti_link, dem;
    CREATE LINKED TABLE bdalti_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT distinct b.* FROM bd_alti.pt3d_alti_d$codeDepFormat b, noisemodelling.$table_infra i WHERE ST_EXPAND(B.THE_GEOM, $buffer) && i.THE_GEOM AND ST_DISTANCE(b.the_geom, i.the_geom) < $buffer)');
    CREATE TABLE dem AS SELECT *, 'DEM' as SOURCE FROM bdalti_link;


    ------------
    -- Import orography

    DROP TABLE IF EXISTS bdtopo_oro_link, bdtopo_oro;
    CREATE LINKED TABLE bdtopo_oro_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM FROM bd_topo.$table_bd_topo_oro r,
            (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where R.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, E.THE_GEOM) < 1000 AND st_zmin(R.THE_GEOM) > 0)');

    -- Remove objects that are far FROM studied roads
    CREATE TABLE bdtopo_oro AS SELECT * FROM bdtopo_oro_link;
    DELETE FROM bdtopo_oro B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    ALTER TABLE bdtopo_oro ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_oro add primary key(pk_line);


    ------------
    -- Import hydrography

    DROP TABLE IF EXISTS bdtopo_hydro_link, bdtopo_hydro;
    CREATE LINKED TABLE bdtopo_hydro_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM FROM bd_topo.$table_bd_topo_hydro r,
            (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where R.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, E.THE_GEOM) < 1000 AND st_zmin(R.THE_GEOM) > 0)');

    CREATE TABLE bdtopo_hydro AS SELECT * FROM bdtopo_hydro_link;
    DELETE FROM bdtopo_hydro B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    ALTER TABLE bdtopo_hydro ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_hydro add primary key(pk_line);


    ------------
    -- Import roads (that are on the floor --> POS_SOL=0)

    DROP TABLE IF EXISTS bdtopo_route_link, bdtopo_route;
    CREATE LINKED TABLE bdtopo_route_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM, r.LARGEUR FROM bd_topo.$table_bd_topo_route r,
            (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where r.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(r.THE_GEOM, e.THE_GEOM) < 1000 AND r.POS_SOL = ''0'' AND st_zmin(r.THE_GEOM) > 0)');
    
    -- Road width is precalculated into WIDTH column. When largeur < 3, then 3m
    CREATE TABLE bdtopo_route AS SELECT THE_GEOM, (CASE WHEN LARGEUR>3 THEN LARGEUR/2 ELSE 1.5 END) as WIDTH FROM bdtopo_route_link;
    DELETE FROM bdtopo_route B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    CREATE SPATIAL INDEX ON bdtopo_route(the_geom);
    ALTER TABLE bdtopo_route ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_route add primary key(pk_line);


    ------------
    -- Import railways (that are on the floor --> POS_SOL=0)

    DROP TABLE IF EXISTS bdtopo_rail_link, bdtopo_rail;
    CREATE LINKED TABLE bdtopo_rail_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.* FROM noisemodelling."$table_rail" r,
            (select ST_BUFFER(the_geom, $buffer) the_geom FROM noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where r.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(r.THE_GEOM, e.THE_GEOM) < 1000 AND st_zmin(r.THE_GEOM) > 0)');
    
    CREATE TABLE bdtopo_rail AS SELECT a.THE_GEOM, a.LARGEMPRIS, b.* FROM bdtopo_rail_link a, PLATEFORM b WHERE b.IDPLATFORM ='SNCF';
    DELETE FROM bdtopo_rail B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    CREATE SPATIAL INDEX ON bdtopo_rail(the_geom);
    ALTER TABLE bdtopo_rail ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_rail add primary key(pk_line);


    ----------------------------------
    -- Remove non-needed linked tables
    DROP TABLE IF EXISTS bdalti_link, bdtopo_oro_link, bdtopo_hydro_link, bdtopo_route_link, bdtopo_rail_link;
    
    ----------------------------------
    -- Enrich the DEM

    ------------
    -- Insert orography into DEM

    --INSERT INTO DEM(THE_GEOM, SOURCE) SELECT THE_GEOM, 'ORO' FROM ST_EXPLODE('(Select ST_ToMultiPoint(ST_Densify(st_force2D(THE_GEOM),5)) the_geom FROM BDTOPO_ORO)');

    DROP TABLE IF EXISTS BDTOPO_ORO_DENSIFY;
    CREATE TABLE BDTOPO_ORO_DENSIFY AS SELECT ST_ToMultiPoint(ST_Densify(st_force2D(the_geom), 5 )) the_geom, pk_line FROM BDTOPO_ORO where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'ORO' FROM ST_EXPLODE('BDTOPO_ORO_DENSIFY') P, BDTOPO_ORO L WHERE P.pk_line = L.pk_line;
    DROP TABLE IF EXISTS BDTOPO_ORO_DENSIFY;


    ------------
    -- Insert hydrography into DEM

    DROP TABLE IF EXISTS BDTOPO_HYDRO_DENSIFY;
    CREATE TABLE BDTOPO_HYDRO_DENSIFY AS SELECT ST_ToMultiPoint(ST_Densify(st_force2D(the_geom), 5 )) the_geom, pk_line FROM BDTOPO_HYDRO where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'HYD' FROM ST_EXPLODE('BDTOPO_HYDRO_DENSIFY') P, BDTOPO_HYDRO L WHERE P.pk_line = L.pk_line;
    DROP TABLE IF EXISTS BDTOPO_HYDRO_DENSIFY;

    
    ------------
    -- Insert roads into DEM

    DROP TABLE DEM_WITHOUT_PTLINE IF EXISTS;
    CREATE TABLE DEM_WITHOUT_PTLINE(the_geom geometry(POINTZ, $srid), source varchar) AS SELECT st_setsrid(THE_GEOM, $srid), SOURCE FROM DEM;
    -- Remove DEM points that are less than "WIDTH" far FROM roads
    DELETE FROM DEM_WITHOUT_PTLINE WHERE EXISTS (SELECT 1 FROM bdtopo_route b WHERE ST_EXPAND(DEM_WITHOUT_PTLINE.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM_WITHOUT_PTLINE.THE_GEOM, b.the_geom)< b.WIDTH+5 LIMIT 1) ;
    
    -- Create buffer points FROM roads and copy the elevation FROM the roads to the point
    DROP TABLE IF EXISTS BUFFERED_PTLINE;
    -- The buffer size correspond to the greatest value between "largeur" and 3m. If "largeur" is null or lower than 3m, then 3m is returned
    CREATE TABLE BUFFERED_PTLINE AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(st_force2D(the_geom), 2), WIDTH, 'endcap=flat join=mitre'), 5)) the_geom, pk_line FROM bdtopo_route  where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))), $srid) THE_GEOM, 'ROU' FROM ST_EXPLODE('BUFFERED_PTLINE') P, bdtopo_route L WHERE P.PK_LINE = L.PK_LINE;
   
    ------------
    -- Insert rail platform into DEM

    -- Remove DEM points that are less than "LARGEMPRIS/2" far FROM rails
    DELETE FROM DEM_WITHOUT_PTLINE WHERE EXISTS (SELECT 1 FROM bdtopo_rail b WHERE ST_EXPAND(DEM_WITHOUT_PTLINE.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM_WITHOUT_PTLINE.THE_GEOM, b.the_geom)< ((b.LARGEMPRIS/2) + 5)  LIMIT 1) ;
    
    -- Create buffer points FROM rails and copy the elevation FROM the rails to the point
    DROP TABLE IF EXISTS BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;
    -- The buffer size correspond to 
    -- d2 = (LARGEMPRIS - 5.5)/2
    CREATE TABLE BUFFERED_D2 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(st_force2D(the_geom), 2), (LARGEMPRIS - 5.5)/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line FROM bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D2') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;
    
    -- d3 = (LARGEMPRIS - 4)/2
    CREATE TABLE BUFFERED_D3 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 4)/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line FROM bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D3') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;

    -- d4 = (LARGEMPRIS)/2
    CREATE TABLE BUFFERED_D4 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), LARGEMPRIS/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line FROM bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D4') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;

    
    DROP TABLE IF EXISTS DEM;
    ALTER TABLE DEM_WITHOUT_PTLINE RENAME TO DEM;
    CREATE SPATIAL INDEX ON DEM (THE_GEOM);

    ----------------------------------
    -- Remove non needed tables
    
    DROP TABLE PVMT, INFRA, BDTOPO_ROUTE, BDTOPO_RAIL, BDTOPO_HYDRO, BDTOPO_ORO, BUFFERED_D2, BUFFERED_D3, BUFFERED_D4, BUFFERED_PTLINE;

    """













    def queries_test = """
    DROP TABLE IF EXISTS n_troncon_de_route_bdt_000_2023_link, roads;
    CREATE LINKED TABLE n_troncon_de_route_bdt_000_2023_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT geom as "THE_GEOM", cleabs as "ID_ROAD", largeur_de_chaussee as "WIDTH" FROM cbs_uge_input.n_troncon_de_route_bdt_000_2023 WHERE SUBSTRING (insee_commune_gauche, 1, 2)=''$codeDep'')');
 
    CREATE TABLE roads AS SELECT * FROM n_troncon_de_route_bdt_000_2023_link;
    ALTER TABLE roads ADD COLUMN pk serial PRIMARY KEY;
    CREATE spatial index ON roads (THE_GEOM);
    DROP TABLE IF EXISTS n_troncon_de_route_bdt_000_2023_link;

    DROP TABLE IF EXISTS n_batiment_bdt_000_2023_link, buildings;
    CREATE LINKED TABLE n_batiment_bdt_000_2023_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','cbs_uge_input', 
        '(SELECT geom as "THE_GEOM", cleabs as "ID_BAT", hauteur as "HEIGHT" FROM cbs_uge_input.nm_bati_22_test)');

    CREATE TABLE buildings AS SELECT * FROM n_batiment_bdt_000_2023_link;
    ALTER TABLE buildings ADD COLUMN pk serial PRIMARY KEY;
    CREATE spatial index ON buildings (THE_GEOM);
    DROP TABLE IF EXISTS n_batiment_bdt_000_2023_link;

    """

    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "codeDep": codeDep, "table_bd_topo_route" : table_bd_topo_route]


    StringBuilder stringBuilder = new StringBuilder()
    // print to command window
    def engine = new SimpleTemplateEngine()
    

    stringBuilder.append(queries_conf)
    stringBuilder.append(queries_roads)
    stringBuilder.append(queries_infra)
    stringBuilder.append(queries_buildings)   
//    stringBuilder.append(queries_screens)
//    stringBuilder.append(queries_buildings_screens)  
    stringBuilder.append(queries_landcover)
//    stringBuilder.append(queries_landcover_rail)  
//    stringBuilder.append(queries_dem)



    
    template = engine.createTemplate(stringBuilder.toString()).make(binding)
    parseScript(template.toString(), sql, progress, logger)




    // Remove non needed tables
    
    return "Super boulot" 
    

}
