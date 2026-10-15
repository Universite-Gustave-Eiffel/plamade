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

def exec(Connection connection, Map input, ProgressVisitor progress) {

        Logger logger = LoggerFactory.getLogger(this.class)

        // Fetch PostGIS connection settings from the configuration table
        Sql h2sql = new Sql(connection)
        if(!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
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
                try(ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_ligne")) {
                        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_LIGNE", true, batchSize)
                }
                try(ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_trafic")) {
                        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TRAFIC", true, batchSize)
                }
                try(ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_troncon_l")) {
                        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TRONCON_L", true, batchSize)
                }
                try(ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_vitesse")) {
                        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_VITESSE", true, batchSize)
                }
                try(ResultSet rs = pgConnection.createStatement().executeQuery("SELECT * FROM cbs_uge_input.n_ferroviaire_tunnel")) {
                        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, connection, "N_FERROVIAIRE_TUNNEL", true, batchSize)
                }
        }
        // Generate compatible column names with noisemodelling
        runSqlQuery(connection, """
            DROP TABLE IF EXISTS RAILWAY_TRACK;
            CREATE TABLE RAILWAY_TRACK(idsection SERIAL,the_geom geometry,idtroncon varchar,idligne varchar,numligne varchar, ntrack INTEGER, trackspd DOUBLE, transfer VARCHAR,
             roughness VARCHAR, impact VARCHAR, curvature INTEGER, bridge VARCHAR, istunnel BOOLEAN);
            INSERT INTO RAILWAY_TRACK(the_geom,idtroncon,idligne,numligne, ntrack, trackspd, transfer, roughness, impact, curvature, bridge, istunnel) 
             SELECT
                geom as the_geom,
                t.idtroncon,
                t.idligne,
                t.numligne,
                t.nb_voies as ntrack, 
                t.vmaxinfra as trackspd,
               (CASE   WHEN  T.BASEVOIE = 'C' THEN 5::integer 
                        WHEN T.BASEVOIE = 'W' THEN 7::integer 
                        ELSE 8::integer
                END) as transfer,
                (CASE   WHEN T.RUGOSITE = 'E' THEN 1::integer 
                        ELSE 2::integer
                END) as roughness,
                (CASE   WHEN T.JOINTRAIL = 'N' THEN 0::integer 
                        ELSE 1::integer 
                END) as impact,  
                0::integer as curvature,
                '' as bridge,
                idtunnel is not null as istunnel
            FROM N_ferroviaire_TRONCON_L T LEFT JOIN n_ferroviaire_tunnel tun ON (T.idtroncon = TUN.idtroncon);
        """)

        //ScriptUtilities.execScript(new Railway_Emission_from_Traffic(), connection, [tableRailwayTraffic:"RAILWAY_TRAFFIC", tableRailwayTrack : "RAILWAY_TRACK"])
}


@CompileStatic
static void runSqlQuery(Connection connection, String query, ProgressVisitor progressVisitor = new EmptyProgressVisitor()) {
        Logger logger = LoggerFactory.getLogger(Thread.currentThread().getName())
        logger.info(ScriptUtilities.execScript(new Execute_Query(),connection, [sqlQueries: query, "outputFormat": "JSON"], progressVisitor) as String)
}