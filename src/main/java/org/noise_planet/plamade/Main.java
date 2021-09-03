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

package org.noise_planet.plamade;

import org.noise_planet.plamade.api.ApiEndpoints;
import org.noise_planet.plamade.api.ApiModule;
import org.noise_planet.plamade.auth.AuthModule;
import org.noise_planet.plamade.config.AuthConfig;
import org.noise_planet.plamade.process.ExecutorServiceModule;
import org.pac4j.oidc.client.GoogleOidcClient;
import ratpack.groovy.template.TextTemplateModule;
import ratpack.guice.Guice;
import ratpack.pac4j.RatpackPac4j;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import ratpack.session.SessionModule;

import ratpack.service.Service;
import ratpack.service.StartEvent;
import ratpack.hikari.HikariModule;
import ratpack.thymeleaf.ThymeleafModule;

import static ratpack.handling.Handlers.redirect;

import javax.sql.DataSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Starts the plamade application.
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
public class Main {
    private static final int DATABASE_VERSION = 1;

    public static void main(String... args) throws Exception {
        Path basePath = BaseDir.find();
        File configPath = new File(basePath.toFile(), "config.yaml");
        if(!configPath.exists()) {
            // Configuration file does not exists
            // Copy example configuration file
            File sourceConfigPath = new File(basePath.toFile(), "config.demo.yaml");
            Files.copy(sourceConfigPath.toPath(), configPath.toPath());
        }

        String databasePath = Paths.get("database").toAbsolutePath().toString();
        RatpackServer.start(s -> s.serverConfig(c -> c.yaml("config.yaml").port(9580).env().require("/auth", AuthConfig.class)
                .baseDir(basePath).build()).registry(Guice.registry(b -> b.module(ApiModule.class)
                .module(AuthModule.class)
                .module(TextTemplateModule.class)
                .module(ThymeleafModule.class)
                .module(SessionModule.class)
                .module(ExecutorServiceModule.class)
                .module(HikariModule.class, hikariConfig -> {
            hikariConfig.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
            hikariConfig.addDataSourceProperty("URL", "jdbc:h2:" + databasePath + ";DB_CLOSE_DELAY=30"); // H2 database path and config
        }).bind(InitDb.class))).handlers(chain ->
                chain.path(redirect(301, "index.html"))
                        .files(files -> files.files("css")) // share all static files from css folder
                        .files(files -> files.files("js"))  //  share all static files from js folder
                        .files(files -> files.files("img"))  //  share all static files from img folder
                        .all(RatpackPac4j.authenticator(chain.getRegistry().get(GoogleOidcClient.class))).insert(ApiEndpoints.class)));
    }

    static class InitDb implements Service {
        public void onStart(StartEvent startEvent) throws Exception {
            DataSource dataSource = startEvent.getRegistry().get(DataSource.class);
            try (Connection connection = dataSource.getConnection()) {
                Statement st = connection.createStatement();
                st.executeUpdate("CREATE TABLE IF NOT EXISTS ATTRIBUTES(DATABASE_VERSION INTEGER)");
                ResultSet rs = st.executeQuery("SELECT * FROM ATTRIBUTES");
                int databaseVersion;
                if(rs.next()) {
                    databaseVersion = rs.getInt("DATABASE_VERSION");
                } else {
                    databaseVersion = DATABASE_VERSION;
                    PreparedStatement pst = connection.prepareStatement("INSERT INTO ATTRIBUTES(DATABASE_VERSION) VALUES(?);");
                    pst.setInt(1, DATABASE_VERSION);
                    pst.execute();
                    // First database
                    st.executeUpdate("CREATE TABLE USERS(PK_USER SERIAL, USER_OID VARCHAR)");
                    st.executeUpdate("CREATE TABLE USER_ASK_INVITATION(PK_INVITE SERIAL, USER_OID VARCHAR, MAIL VARCHAR)");
                    st.executeUpdate("CREATE TABLE IF NOT EXISTS JOBS(PK_JOB SERIAL,REMOTE_JOB_FOLDER VARCHAR NOT NULL," +
                            " BEGIN_DATE TIMESTAMP WITHOUT TIME ZONE, END_DATE TIMESTAMP WITHOUT TIME ZONE, PROGRESSION REAL DEFAULT 0, CONF_ID INTEGER, INSEE_DEPARTMENT VARCHAR, PK_USER INTEGER NOT NULL)");

                }
                // In the future check databaseVersion for database upgrades
                if(databaseVersion != DATABASE_VERSION) {
                    st.executeUpdate("UPDATE ATTRIBUTES SET DATABASE_VERSION = " + DATABASE_VERSION);
                }
            }
        }
    }
}
