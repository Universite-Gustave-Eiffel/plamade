/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Field
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities
import org.noise_planet.noisemodelling.jdbc.utils.DataBaseUtilities
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query
import org.noise_planet.noisemodelling.scripts.NoiseModelling.Railway_Emission_from_Traffic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet

title = 'Create trains sounds sources table in PostGIS database'
description = 'Create trains sounds sources table in PostGIS database'

inputs = [
        projectionName: [
                description: "Projection name",
                title: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

@Field
static final int batchSize = 100

@Field
public static final String RailwayTrainsets = "/org/noise_planet/covadis/scripts/CBS/RailwayTrainsets.json"

@Field
public static final String RailwayVehicles = "/org/noise_planet/covadis/scripts/CBS/RailwayVehiclesCnossos.json"

def exec(Connection connection, Map input, ProgressVisitor progress) {

        Logger logger = LoggerFactory.getLogger(this.class)

        // Fetch PostGIS connection settings from the configuration table
        Sql h2sql = new Sql(connection)
        if (!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
                throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
        }

        def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

        try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
                postgisConfig['user_name'] as String,
                postgisConfig['password'] as String,
                postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
             Connection pgConnection = dataSource.getConnection()) {
                logger.info("Connected to PostgreSQL database")
                pgConnection.setAutoCommit(true)
                // Fetch train data tables
                copyTrainTableFromPostgres(pgConnection, connection)

                convertTrafficToNoiseSources(connection)

                // Copy output table to postgis
                try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM LW_RAILWAY")) {
                        PostGISUtilities.copyResultSetToDatabase(connection, resultSet, pgConnection, "cbs_uge_output.ferroviaire_emission_hexa", true, batchSize);
                }
                try (ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM RAILWAY_TRACK")) {
                        PostGISUtilities.copyResultSetToDatabase(connection, resultSet, pgConnection, "cbs_uge_output.ferroviaire_troncon_hexa", true, batchSize);
                }
                runSqlQuery(pgConnection, """
                    -- Create index on remote database                                       
                    CREATE INDEX ON cbs_uge_output.ferroviaire_emission_hexa  USING GIST (THE_GEOM);
                    CREATE INDEX ON cbs_uge_output.ferroviaire_troncon_hexa  USING GIST (THE_GEOM);
                    CREATE INDEX ON cbs_uge_output.ferroviaire_emission_hexa (idtroncon);
                    CREATE INDEX ON cbs_uge_output.ferroviaire_troncon_hexa (idsection);
                    CREATE INDEX ON cbs_uge_output.ferroviaire_troncon_hexa (uueid);
                    ALTER TABLE cbs_uge_output.ferroviaire_emission_hexa OWNER TO cbs_uge_group;                   
                    ALTER TABLE cbs_uge_output.ferroviaire_troncon_hexa OWNER TO cbs_uge_group;

                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.pk_section IS 'Primary key of the track section';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.the_geom IS 'Geometry of the railway section';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.idsection IS 'Identifier of the original track section (idtroncon from N_FERROVIAIRE_TRONCON_L)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.ntrack IS 'Number of tracks';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.trackspd IS 'Maximum infrastructure speed on the section (in km/h)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.transfer IS 'Track transfer function identifier, e.g. SNCF5, SNCF7, SNCF8 (steel bridge)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.roughness IS 'Rail roughness identifier, e.g. SNCF1 or SNCF2';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.impact IS 'Impact noise coefficient identifier, e.g. SNCF1, empty for none';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.curvature IS 'Listed code describing the curvature of the section';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.bridge IS 'Bridge transfer function identifier, empty for none';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.istunnel IS 'Indicates whether the section is a tunnel or not (0 = no / 1 = yes)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.comspd IS 'Commercial speed on the section (in km/h)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.platform IS 'Platform/line type: SNCF or SNCF_LGV (high speed line)';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.uueid IS 'Identifier of the infrastructure';
                    COMMENT ON COLUMN cbs_uge_output.ferroviaire_troncon_hexa.pos_sol IS 'Position index relative to the ground (negative = tunnel)';
                """)
                // Remove local intermediate tables
                runSqlQuery(connection, "DROP TABLE IF EXISTS RAILWAY_TRACK, LW_RAILWAY, RAILWAY_TRAFFIC;");
        }
}

static convertTrafficToNoiseSources(Connection connection) {
        runSqlQuery(connection, """
                    DROP TABLE IF EXISTS RAILWAY_TRACK;
                    CREATE TABLE RAILWAY_TRACK(pk_section SERIAL PRIMARY KEY, the_geom geometry, IDSECTION VARCHAR, ntrack INTEGER, trackspd DOUBLE, transfer VARCHAR,
                     roughness VARCHAR, impact VARCHAR, curvature INTEGER, bridge VARCHAR, istunnel BOOLEAN, comspd DOUBLE, platform VARCHAR, uueid VARCHAR, pos_sol VARCHAR);
                     
                    INSERT INTO RAILWAY_TRACK(the_geom, IDSECTION, ntrack, trackspd, transfer, roughness, impact, curvature, bridge, istunnel, comspd, platform, uueid, pos_sol) 
                     SELECT
                        geom as the_geom,
                        t.idtroncon as IDSECTION,
                        t.nb_voies as ntrack, 
                        t.vmaxinfra as trackspd,
                       (CASE   WHEN  PMT.idpont IS NOT NULL THEN 'SNCF8'
                                WHEN T.BASEVOIE = 'C' THEN 'SNCF5' 
                                WHEN T.BASEVOIE = 'W' THEN 'SNCF7' 
                                ELSE 'SNCF5'
                        END) as transfer,
                        (CASE   WHEN T.RUGOSITE = 'E' THEN 'SNCF1'
                                ELSE 'SNCF2'
                        END) as roughness,
                        (CASE   WHEN T.JOINTRAIL = 'N' THEN '' 
                                ELSE 'SNCF1' 
                        END) as impact,  
                        0::integer as curvature,
                        '' as bridge,
                        pos_sol LIKE '-%' as istunnel,
                        VITESSE as comspd,
                        (CASE WHEN ligne.typeligne = '01' THEN 'SNCF' ELSE 'SNCF_LGV' END) as PLATFORM,
                        ligne.uueid,
                        t.pos_sol 
                    FROM N_ferroviaire_TRONCON_L T 
                     INNER JOIN N_ferroviaire_vitesse V ON (T.idtroncon = V.idtroncon)
                     INNER JOIN n_ferroviaire_ligne ligne ON (T.idligne = ligne.idligne)
                     LEFT JOIN N_ferroviaire_pont_metallique pmt ON (T.idtroncon = PMT.idtroncon);
                    CREATE UNIQUE INDEX ON RAILWAY_TRACK (IDSECTION);
                    
                    DROP TABLE IF EXISTS RAILWAY_TRAFFIC;
                    CREATE TABLE RAILWAY_TRAFFIC (PKTRAFFIC SERIAL PRIMARY KEY, IDSECTION VARCHAR, TRAINTYPE VARCHAR,
                     TRAINSPD DOUBLE, TDAY DOUBLE, TEVENING DOUBLE, TNIGHT DOUBLE);
                     
                    INSERT INTO RAILWAY_TRAFFIC(IDSECTION, TRAINTYPE, TRAINSPD, TDAY, TEVENING, TNIGHT)
                      SELECT IDTRONCON, ENGMOTEUR, VMAX, (TDIURNE/12.0), (TSOIR/4.0), (TNUIT/8.0)
                      FROM N_FERROVIAIRE_TRAFIC;
                      CREATE INDEX ON RAILWAY_TRAFFIC (IDSECTION);
                      
                    DROP TABLE n_ferroviaire_troncon_l, n_ferroviaire_vitesse, n_ferroviaire_ligne, n_ferroviaire_tunnel, n_ferroviaire_pont_metallique, n_ferroviaire_trafic;        
                        """)

        ScriptUtilities.execScript(new Railway_Emission_from_Traffic(), connection, [
                tableRailwayTraffic: "RAILWAY_TRAFFIC",
                tableRailwayTrack: "RAILWAY_TRACK",
                trainSetDataFile : RailwayTrainsets,
                vehicleDataFile : RailwayVehicles
                ]
        )

        // Add a column to insert the idsection from original track idtroncon
        runSqlQuery(connection, """
                ALTER TABLE LW_RAILWAY ADD COLUMN idtroncon VARCHAR;
                UPDATE LW_RAILWAY SET idtroncon = (SELECT idsection FROM RAILWAY_TRACK R WHERE R.pk_section = LW_RAILWAY.pk_section);
                """, new EmptyProgressVisitor())
}

static copyTrainTableFromPostgres(Connection pgConnection, Connection connection) {
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_ligne")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_LIGNE", true, batchSize)
        }
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_trafic")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TRAFIC", true, batchSize)
        }
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_troncon_l")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TRONCON_L", true, batchSize)
        }
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_vitesse")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_VITESSE", true, batchSize)
        }
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_tunnel")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TUNNEL", true, batchSize)
        }
        try (ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_pont_metallique")) {
                PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_PONT_METALLIQUE", true, batchSize)
        }
}

@CompileStatic
static void runSqlQuery(Connection connection, String query, ProgressVisitor progressVisitor = new EmptyProgressVisitor()) {
        Logger logger = LoggerFactory.getLogger(Thread.currentThread().getName())
        DataBaseUtilities.parseScript(query, connection, progressVisitor, logger)
}