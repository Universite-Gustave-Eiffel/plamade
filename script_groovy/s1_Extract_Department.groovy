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

import groovy.text.SimpleTemplateEngine
import groovy.transform.CompileStatic
import org.h2.util.ScriptReader
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
        inputServer : [
                name       : 'DB Server used',
                title      : 'DB server used',
                description: 'Choose between cerema or cloud',
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
static def parseScript(String sqlInstructions, Sql sql) {
    Reader reader = null
    ByteArrayInputStream s = new ByteArrayInputStream(sqlInstructions.getBytes())
    InputStream is = s
    try {
        reader  = new InputStreamReader(is)
        ScriptReader scriptReader = new ScriptReader(reader)
        String statement = scriptReader.readStatement()
        while (statement != null) {
            sql.execute(statement)
            statement = scriptReader.readStatement()
        }
    } finally {
        reader.close()
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

    def databaseUrl
    if(input["inputServer"].equals('cerema')) {
        databaseUrl="jdbc:postgresql_h2://161.48.203.166:5432/plamade?ssl=true&sslmode=prefer"
    } else if(input["inputServer"].equals('cloud')) {
        databaseUrl = "jdbc:postgresql_h2://57.100.98.126:5432/plamade?ssl=true&sslmode=prefer"
    } else{
        return "Vous n'avez pas spécifié le bon nom de serveur"
    }

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
    def table_agglo = "agglo_2154"
    def table_land = "C_NATURESOL_S_2154"
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
    CREATE LINKED TABLE nuts_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
        '(SELECT code_2021 as nuts, ratio_pop_log FROM noisemodelling.nuts WHERE code_dept=''$codeDepFormat'')');

    CREATE TABLE metadata (code_dept varchar, nuts varchar, ratio_pop_log double, srid integer, import_start timestamp, import_end timestamp, 
        grid_conf integer, grid_start timestamp, grid_end timestamp, 
        emi_conf integer, emi_start timestamp, emi_end timestamp, 
        road_conf integer, road_start timestamp, road_end timestamp, 
        rail_conf integer, rail_start timestamp, rail_end timestamp);

    INSERT INTO metadata (code_dept, nuts, ratio_pop_log, srid, import_start) VALUES ('$codeDep', (SELECT nuts from nuts_link), (SELECT ratio_pop_log from nuts_link), $srid, NOW());
    
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
     b."TMHVLD" as lv_d, b."TMHVLS" as lv_e, b."TMHVLN" as lv_n,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLD" * b."PCENTMPL"/b."PCENTPL" ELSE 0 END) as mv_d,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLS" * b."PCENTMPL"/b."PCENTPL" ELSE 0 END) as mv_e,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLN" * b."PCENTMPL"/b."PCENTPL" ELSE 0 END) as mv_n,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLD" * b."PCENTHPL"/b."PCENTPL" ELSE 0 END) as hgv_d,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLS" * b."PCENTHPL"/b."PCENTPL" ELSE 0 END) as hgv_e,
     (CASE  WHEN b."PCENTPL" > 0 THEN b."TMHPLN" * b."PCENTHPL"/b."PCENTPL" ELSE 0 END) as hgv_n,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RD" * b."PCENT2R4A"/b."PCENT2R" ELSE 0 END) as wav_d,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RS" * b."PCENT2R4A"/b."PCENT2R" ELSE 0 END) as wav_e,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RN" * b."PCENT2R4A"/b."PCENT2R" ELSE 0 END) as wav_n,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RD" * b."PCENT2R4B"/b."PCENT2R" ELSE 0 END) as wbv_d,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RS" * b."PCENT2R4B"/b."PCENT2R" ELSE 0 END) as wbv_e,
     (CASE  WHEN b."PCENT2R" > 0 THEN b."TMH2RN" * b."PCENT2R4B"/b."PCENT2R" ELSE 0 END) as wbv_n,
     c."VITESSEVL" as lv_spd_d, c."VITESSEVL" as lv_spd_e, c."VITESSEVL" as lv_spd_n, 
     c."VITESSEPL" as mv_spd_d,c."VITESSEPL" as mv_spd_e, c."VITESSEPL" as mv_spd_n, 
     c."VITESSEPL" as hgv_spd_d, c."VITESSEPL" as hgv_spd_e, c."VITESSEPL" as hgv_spd_n, 
     c."VITESSE4A" as wav_spd_d, c."VITESSE4A" as wav_spd_e, c."VITESSE4A" as wav_spd_n, 
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
     echeance4."N_ROUTIER_ROUTE" f 
    WHERE 
     ST_LENGTH(a.the_geom)>0 and
     a."CODEDEPT" = lpad(''$codeDep'',3,''0'') and
     a."CBS_GITT" and
     f."CONCESSION"=''N'' and 
     a."IDTRONCON"=b."IDTRONCON" and
     a."IDTRONCON"=c."IDTRONCON" and
     a."IDTRONCON"=d."IDTRONCON" and 
     a."IDROUTE"=f."IDROUTE")');
    
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
                WHEN a."BASEVOIE" = ''N'' THEN 8::integer
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
        a."LARGEMPRIS" - 5.5 as d2,
        a."LARGEMPRIS" - 4 as d3,
        a."LARGEMPRIS" as d4,
        b."VITESSE" as comspd,
        c."TYPELIGNE" as linetype,
        (CASE   WHEN c."TYPELIGNE" = ''01'' THEN 3.67::float 
                WHEN c."TYPELIGNE" = ''02'' THEN 4.5::float 
        END) as trackspc,
        c."UUEID" as uueid,
        d.idplatform, d.d1, d.g1, d.g2, d.g3, d.h1, d.h2
    FROM 
        noisemodelling."$table_rail" a,
        echeance4."N_FERROVIAIRE_VITESSE" b,
        echeance4."N_FERROVIAIRE_LIGNE" c,
        noisemodelling.platform d     
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CODEDEPT" = lpad(''$codeDep'',3,''0'') and
        a."CBS_GITT" and
        a."REFPROD"=''412280737'' and  
        a."IDTRONCON" = b."IDTRONCON" and
        a."IDLIGNE" = c."IDLIGNE" and
        d.idplatform=''SNCF'')');

    CREATE TABLE rail_sections_geom AS SELECT * FROM rail_sections_link;

    ALTER TABLE rail_sections_geom ADD COLUMN bridge integer DEFAULT 0;

    CREATE LINKED TABLE rail_tunnel_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','echeance4', 
    '(SELECT
        "IDTUNNEL" as idtunnel, 
        "IDTRONCON" as idsection 
    FROM 
        echeance4."N_FERROVIAIRE_TUNNEL")');

    CREATE TABLE rail_tunnel AS SELECT * FROM rail_tunnel_link;

    CREATE TABLE rail_sections AS SELECT ST_SETSRID(a.THE_GEOM,$srid) as THE_GEOM, a.idsection, a.ntrack, a.idline, a.numline, a.trackspd, a.transfer, a.roughness, a.impact, a.curvature, a.bridge, a.d2, a.d3, a.d4, a.comspd, a.linetype, a.trackspc, a.uueid, a.idplatform, a.d1, a.g1, a.g2, a.g3, a.h1, a.h2, b.idtunnel FROM rail_sections_geom a LEFT JOIN rail_tunnel b ON a.idsection = b.idsection;
    ALTER TABLE rail_sections ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX rail_sections_geom_idx ON rail_sections (the_geom);
    CREATE INDEX ON rail_sections (idsection);
    CREATE INDEX ON rail_sections(uueid);

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
     b."POP_BAT" as pop,
     a."AGGLO" as agglo 
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
	CREATE TABLE buildings as SELECT a.the_geom, a.id_bat, a.bat_uueid, a.height, a.pop, a.agglo, b.id_erps, b.erps_natur FROM buildings_geom a LEFT JOIN buildings_erps b ON a.id_bat = b.id_bat;
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
    ALTER TABLE screens ADD COLUMN agglo boolean DEFAULT false;
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
    CREATE TABLE tmp_screen_truncated AS SELECT pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 0.5)) the_geom, s.id_bat, s.bat_uueid, s.height, s.pop, s.agglo, s.id_erps, s.erps_natur, s.g, s.origin 
        FROM tmp_relation_screen_building r, buildings b, screens s 
        WHERE pk_building = b.pk AND pk_screen = s.pk 
        GROUP BY pk_screen, s.id_bat, s.bat_uueid, s.height, s.pop, s.id_erps, s.erps_natur, s.g, s.origin;

    -- Merge untruncated screens and truncated screens
    CREATE TABLE tmp_screens AS 
        SELECT the_geom, pk, id_bat, bat_uueid, height, pop, agglo, id_erps, erps_natur, g, origin FROM screens WHERE pk not in (SELECT pk_screen FROM tmp_screen_truncated) UNION ALL 
        SELECT the_geom, pk_screen as pk, id_bat, bat_uueid, height, pop, agglo, id_erps, erps_natur, g, origin FROM tmp_screen_truncated;

    -- Convert linestring screens to polygons with buffer function
    CREATE TABLE tmp_buffered_screens AS SELECT ST_SETSRID(ST_BUFFER(sc.the_geom, 0.1, 'join=mitre endcap=flat'), ST_SRID(sc.the_geom)) as the_geom, pk, id_bat, bat_uueid, height, pop, agglo, id_erps, erps_natur, g, origin 
        FROM tmp_screens sc;

    -- Merge buildings and buffered screens
    CREATE TABLE buildings_screens as 
        SELECT the_geom, id_bat, bat_uueid, height, pop, agglo, id_erps, erps_natur, g, origin FROM tmp_buffered_screens sc UNION ALL 
        SELECT the_geom, id_bat, bat_uueid, height, pop, agglo, id_erps, erps_natur, g, origin FROM buildings;

    ALTER TABLE buildings_screens ADD COLUMN pk serial PRIMARY KEY;
    CREATE SPATIAL INDEX ON buildings_screens(the_geom);

    DROP TABLE IF EXISTS tmp_relation_screen_building, tmp_screen_truncated, tmp_screens, tmp_buffered_screens, buffered_screens;

    """

    def queries_landcover = """
	----------------------------------
	-- Manage Landcover
    
	DROP TABLE IF EXISTS alllandcover_link, LANDCOVER;
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
    
    CREATE TABLE LANDCOVER as select * from alllandcover_link;
    CREATE SPATIAL INDEX ON LANDCOVER(the_geom);
    DELETE FROM LANDCOVER B WHERE NOT EXISTS (SELECT 1 FROM infra R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    DROP TABLE alllandcover_link;

    """

    def queries_landcover_rail = """

    -- Integrates RAIL_SECTIONS into the Landcover
    ------------------------------------------------------------------

    DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4;
    CREATE TABLE rail_buff_d1 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, d1/2))) as the_geom FROM rail_sections;
    CREATE TABLE rail_buff_d3 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, d3/2))) as the_geom FROM rail_sections;
    CREATE TABLE rail_buff_d4 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, d4/2))) as the_geom FROM rail_sections;

    DROP TABLE IF EXISTS rail_diff_d3_d1, rail_diff_d4_d3;
    CREATE TABLE rail_diff_d3_d1 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d3 a, rail_buff_d1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
    CREATE TABLE rail_diff_d4_d3 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d4 a, rail_buff_d3 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

    DROP TABLE IF EXISTS rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl;
    CREATE TABLE rail_buff_d1_expl AS SELECT a.the_geom, b.g3 as g FROM ST_Explode('RAIL_BUFF_D1') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
    CREATE TABLE rail_buff_d3_expl AS SELECT a.the_geom, b.g2 as g FROM ST_Explode('RAIL_DIFF_D3_D1 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
    CREATE TABLE rail_buff_d4_expl AS SELECT a.the_geom, b.g1 as g FROM ST_Explode('RAIL_DIFF_D4_D3 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';

    DROP TABLE IF EXISTS LANDCOVER_G_0, LANDCOVER_G_03, LANDCOVER_G_07, LANDCOVER_G_1;
    CREATE TABLE LANDCOVER_G_0 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER WHERE g=0;
    CREATE TABLE LANDCOVER_G_03 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER WHERE g=0.3;
    CREATE TABLE LANDCOVER_G_07 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER WHERE g=0.7;
    CREATE TABLE LANDCOVER_G_1 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER WHERE g=1;

    DROP TABLE IF EXISTS LANDCOVER_0_DIFF_D4, LANDCOVER_03_DIFF_D4, LANDCOVER_07_DIFF_D4, LANDCOVER_1_DIFF_D4;
    CREATE TABLE LANDCOVER_0_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_0 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
    CREATE TABLE LANDCOVER_03_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_03 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
    CREATE TABLE LANDCOVER_07_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_07 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
    CREATE TABLE LANDCOVER_1_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

    DROP TABLE IF EXISTS LANDCOVER_0_EXPL, LANDCOVER_03_EXPL, LANDCOVER_07_EXPL, LANDCOVER_1_EXPL;
    CREATE TABLE LANDCOVER_0_EXPL AS SELECT the_geom, 0 as g FROM ST_Explode('LANDCOVER_0_DIFF_D4 ');
    CREATE TABLE LANDCOVER_03_EXPL AS SELECT the_geom, 0.3 as g FROM ST_Explode('LANDCOVER_03_DIFF_D4 ');
    CREATE TABLE LANDCOVER_07_EXPL AS SELECT the_geom, 0.7 as g FROM ST_Explode('LANDCOVER_07_DIFF_D4 ');
    CREATE TABLE LANDCOVER_1_EXPL AS SELECT the_geom, 1 as g FROM ST_Explode('LANDCOVER_1_DIFF_D4 ');

    -- Unifiy tables
    DROP TABLE IF EXISTS LANDCOVER_UNION, LANDCOVER_MERGE, LANDCOVER;
    CREATE TABLE LANDCOVER_UNION AS SELECT * FROM LANDCOVER_0_EXPL UNION SELECT * FROM LANDCOVER_03_EXPL UNION SELECT * FROM LANDCOVER_07_EXPL 
    UNION SELECT * FROM LANDCOVER_1_EXPL UNION SELECT * FROM RAIL_BUFF_D1_EXPL UNION SELECT * FROM RAIL_BUFF_D3_EXPL UNION SELECT * FROM RAIL_BUFF_D4_EXPL ; 

    -- Merge geometries that have the same G
    CREATE TABLE LANDCOVER_MERGE AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, g FROM LANDCOVER_UNION GROUP BY g;
    DROP TABLE IF EXISTS LANDCOVER;
    CREATE TABLE LANDCOVER AS SELECT ST_SETSRID(the_geom,$srid) as the_geom, g FROM ST_Explode('LANDCOVER_MERGE');

    DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4, rail_diff_d3_d1, rail_diff_d4_d3, rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl, LANDCOVER_G_0, LANDCOVER_G_03, LANDCOVER_G_07, LANDCOVER_G_1, LANDCOVER_0_DIFF_D4, LANDCOVER_03_DIFF_D4, LANDCOVER_07_DIFF_D4, LANDCOVER_1_DIFF_D4, LANDCOVER_0_EXPL, LANDCOVER_03_EXPL, LANDCOVER_07_EXPL, LANDCOVER_1_EXPL, LANDCOVER_UNION, LANDCOVER_MERGE, LANDCOVER;

    """

    def queries_dem = """
     ----------------------------------
    -- Import data and filtering within 1000m around infra

    ------------
    -- Import DEM from BD Alti

    DROP TABLE IF EXISTS bdalti_link, dem;
    CREATE LINKED TABLE bdalti_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
    '(SELECT distinct b.* FROM bd_alti.pt3d_alti_d$codeDepFormat b, noisemodelling.$table_infra i WHERE ST_EXPAND(B.THE_GEOM, $buffer) && i.THE_GEOM AND ST_DISTANCE(b.the_geom, i.the_geom) < $buffer)');
    CREATE TABLE dem AS SELECT *, 'DEM' as SOURCE FROM bdalti_link;


    ------------
    -- Import orography

    DROP TABLE IF EXISTS bdtopo_oro_link, bdtopo_oro;
    CREATE LINKED TABLE bdtopo_oro_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM FROM bd_topo.$table_bd_topo_oro r,
            (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where R.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, E.THE_GEOM) < 1000 AND st_zmin(R.THE_GEOM) > 0)');

    -- Remove objects that are far from studied roads
    CREATE TABLE bdtopo_oro AS SELECT * FROM bdtopo_oro_link;
    DELETE FROM bdtopo_oro B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    ALTER TABLE bdtopo_oro ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_oro add primary key(pk_line);


    ------------
    -- Import hydrography

    DROP TABLE IF EXISTS bdtopo_hydro_link, bdtopo_hydro;
    CREATE LINKED TABLE bdtopo_hydro_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM FROM bd_topo.$table_bd_topo_hydro r,
            (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where R.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, E.THE_GEOM) < 1000 AND st_zmin(R.THE_GEOM) > 0)');

    CREATE TABLE bdtopo_hydro AS SELECT * FROM bdtopo_hydro_link;
    DELETE FROM bdtopo_hydro B WHERE NOT EXISTS (SELECT 1 FROM INFRA R WHERE ST_EXPAND(B.THE_GEOM, $buffer) && R.THE_GEOM AND ST_DISTANCE(b.the_geom, r.the_geom) < $buffer LIMIT 1);
    ALTER TABLE bdtopo_hydro ADD pk_line INT AUTO_INCREMENT NOT NULL;
    ALTER TABLE bdtopo_hydro add primary key(pk_line);


    ------------
    -- Import roads (that are on the floor --> POS_SOL=0)

    DROP TABLE IF EXISTS bdtopo_route_link, bdtopo_route;
    CREATE LINKED TABLE bdtopo_route_link ('org.h2gis.postgis_jts.Driver','$databaseUrl','$user','$pwd','noisemodelling', 
            '(SELECT r.THE_GEOM, r.LARGEUR FROM bd_topo.$table_bd_topo_route r,
            (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where r.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(r.THE_GEOM, e.THE_GEOM) < 1000 AND r.POS_SOL = ''0'' AND st_zmin(r.THE_GEOM) > 0)');
    
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
            (select ST_BUFFER(the_geom, $buffer) the_geom from noisemodelling.$table_dept e WHERE e.insee_dep=''$codeDep'' LIMIT 1) e where r.THE_GEOM && e.THE_GEOM AND ST_DISTANCE(r.THE_GEOM, e.THE_GEOM) < 1000 AND st_zmin(r.THE_GEOM) > 0)');
    
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
    CREATE TABLE BDTOPO_ORO_DENSIFY AS SELECT ST_ToMultiPoint(ST_Densify(st_force2D(the_geom), 5 )) the_geom, pk_line from BDTOPO_ORO where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'ORO' FROM ST_EXPLODE('BDTOPO_ORO_DENSIFY') P, BDTOPO_ORO L WHERE P.pk_line = L.pk_line;
    DROP TABLE IF EXISTS BDTOPO_ORO_DENSIFY;


    ------------
    -- Insert hydrography into DEM

    DROP TABLE IF EXISTS BDTOPO_HYDRO_DENSIFY;
    CREATE TABLE BDTOPO_HYDRO_DENSIFY AS SELECT ST_ToMultiPoint(ST_Densify(st_force2D(the_geom), 5 )) the_geom, pk_line from BDTOPO_HYDRO where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'HYD' FROM ST_EXPLODE('BDTOPO_HYDRO_DENSIFY') P, BDTOPO_HYDRO L WHERE P.pk_line = L.pk_line;
    DROP TABLE IF EXISTS BDTOPO_HYDRO_DENSIFY;

    
    ------------
    -- Insert roads into DEM

    DROP TABLE DEM_WITHOUT_PTLINE IF EXISTS;
    CREATE TABLE DEM_WITHOUT_PTLINE(the_geom geometry(POINTZ, $srid), source varchar) AS SELECT st_setsrid(THE_GEOM, $srid), SOURCE FROM DEM;
    -- Remove DEM points that are less than "WIDTH" far from roads
    DELETE FROM DEM_WITHOUT_PTLINE WHERE EXISTS (SELECT 1 FROM bdtopo_route b WHERE ST_EXPAND(DEM_WITHOUT_PTLINE.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM_WITHOUT_PTLINE.THE_GEOM, b.the_geom)< b.WIDTH+5 LIMIT 1) ;
    
    -- Create buffer points from roads and copy the elevation from the roads to the point
    DROP TABLE IF EXISTS BUFFERED_PTLINE;
    -- The buffer size correspond to the greatest value between "largeur" and 3m. If "largeur" is null or lower than 3m, then 3m is returned
    CREATE TABLE BUFFERED_PTLINE AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(st_force2D(the_geom), 2), WIDTH, 'endcap=flat join=mitre'), 5)) the_geom, pk_line from bdtopo_route  where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))), $srid) THE_GEOM, 'ROU' FROM ST_EXPLODE('BUFFERED_PTLINE') P, bdtopo_route L WHERE P.PK_LINE = L.PK_LINE;
   
    ------------
    -- Insert rail platform into DEM

    -- Remove DEM points that are less than "LARGEMPRIS/2" far from rails
    DELETE FROM DEM_WITHOUT_PTLINE WHERE EXISTS (SELECT 1 FROM bdtopo_rail b WHERE ST_EXPAND(DEM_WITHOUT_PTLINE.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM_WITHOUT_PTLINE.THE_GEOM, b.the_geom)< ((b.LARGEMPRIS/2) + 5)  LIMIT 1) ;
    
    -- Create buffer points from rails and copy the elevation from the rails to the point
    DROP TABLE IF EXISTS BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;
    -- The buffer size correspond to 
    -- d2 = (LARGEMPRIS - 5.5)/2
    CREATE TABLE BUFFERED_D2 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(st_force2D(the_geom), 2), (LARGEMPRIS - 5.5)/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line from bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D2') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;
    
    -- d3 = (LARGEMPRIS - 4)/2
    CREATE TABLE BUFFERED_D3 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 4)/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line from bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D3') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;

    -- d4 = (LARGEMPRIS)/2
    CREATE TABLE BUFFERED_D4 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), LARGEMPRIS/2, 'endcap=flat join=mitre'), 5)) the_geom, pk_line from bdtopo_rail where st_length(st_simplify(the_geom, 2)) > 0 ;
    INSERT INTO DEM_WITHOUT_PTLINE(THE_GEOM, SOURCE) SELECT st_setsrid(ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1), $srid) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D4') P, bdtopo_rail L WHERE P.PK_LINE = L.PK_LINE;

    
    DROP TABLE IF EXISTS DEM;
    ALTER TABLE DEM_WITHOUT_PTLINE RENAME TO DEM;
    CREATE SPATIAL INDEX ON DEM (THE_GEOM);

    ----------------------------------
    -- Remove non needed tables
    
    DROP TABLE PVMT, INFRA, BDTOPO_ROUTE, BDTOPO_RAIL, BDTOPO_HYDRO, BDTOPO_ORO, BUFFERED_D2, BUFFERED_D3, BUFFERED_D4, BUFFERED_PTLINE;

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

    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "codeDep": codeDep, "table_bd_topo_route" : table_bd_topo_route]


    // print to command window
    logger.info('Manage configuration tables (1/11)')
    def engine = new SimpleTemplateEngine()
    def template = engine.createTemplate(queries_conf).make(binding)

    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage PFAV (2/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_pfav).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage roads (3/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_roads).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage rails (4/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_rails).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage infrastructures (5/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_infra).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage buildings (6/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_buildings).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage screens (7/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_screens).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage buildings screens (8/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_buildings_screens).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage landcover (9/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_landcover).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    if(JDBCUtilities.getRowCount(connection, "RAIL_SECTIONS") > 0) {
        logger.info('Manage landcover - There is railways, so insert them into landcover')
        engine = new SimpleTemplateEngine()
        template = engine.createTemplate(queries_landcover_rail).make(binding)
        parseScript(template.toString(), sql)
    }
    progress.endStep()

    logger.info('Manage dem (10/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_dem).make(binding)
    parseScript(template.toString(), sql)
    progress.endStep()

    logger.info('Manage statistics (11/11)')
    engine = new SimpleTemplateEngine()
    template = engine.createTemplate(queries_stats).make(binding)
    parseScript(template.toString(), sql)
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
