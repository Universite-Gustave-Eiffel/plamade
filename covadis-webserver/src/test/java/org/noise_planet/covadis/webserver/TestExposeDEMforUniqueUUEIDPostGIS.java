package org.noise_planet.covadis.webserver;

import com.zaxxer.hikari.HikariDataSource;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.JDBCUtilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ExposeDEMforUniqueUUEID;
import org.noise_planet.covadis.scripts.CBS.Generate_sources;
import org.noise_planet.covadis.scripts.CBS.Write_PostGIS_Settings;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities;
import org.noise_planet.noisemodelling.webserver.database.DatabaseManagement;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test of the ExposeDEMforUniqueUUEID script using a subset of the data
 * extracted from the Plamade PostgreSQL database. The test is a no-op when POSTGRES_HOST is not defined.
 */
public class TestExposeDEMforUniqueUUEIDPostGIS {

    DataSource dataSource;
    DataSource pgDataSource;
    Connection connection;

    @AfterEach
    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (dataSource != null) {
            try {
                dataSource.unwrap(HikariDataSource.class).close();
            } catch (SQLException ex) {
                // ignore
            }
            dataSource = null;
        }
    }

    private static void runSqlFile(Connection pgConnection, String sqlPath) throws IOException, SQLException {
        URL scriptUrl = TestExposeDEMforUniqueUUEIDPostGIS.class.getResource(sqlPath);
        String file = scriptUrl.getFile();
        if (file.endsWith(".zip")) {
            try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(file))) {
                ZipEntry entry = zipInputStream.getNextEntry();
                while (entry != null) {
                    if (!entry.isDirectory()) {
                        executeScript(pgConnection, new String(zipInputStream.readAllBytes()));
                    }
                    entry = zipInputStream.getNextEntry();
                }
            }
        } else {
            try (FileReader fileReader = new FileReader(file)) {
                executeScript(pgConnection, fileReader.readAllAsString());
            }
        }
    }

    private static void executeScript(Connection connection, String script) throws SQLException {
        ScriptReader scriptReader = new ScriptReader(new StringReader(script));
        scriptReader.setSkipRemarks(true);
        String statement = scriptReader.readStatement();
        Statement stmt = connection.createStatement();
        while (statement != null && !StringUtils.isWhitespaceOrEmpty(statement)) {
            stmt.execute(statement);
            statement = scriptReader.readStatement();
        }
    }

    @Test
    public void testExposeDem() throws SQLException, IOException {
        if (System.getenv("POSTGRES_HOST") == null || System.getenv("POSTGRES_HOST").isEmpty()) {
            return; // PostGIS not available
        }
        // H2 in-memory database that holds the POSTGIS_CONFIGURATION table used by the scripts
        dataSource = DatabaseManagement.createH2DataSource("jdbc:h2:mem:junit" + System.nanoTime(), "sa", "sa", "", false);
        connection = JDBCUtilities.wrapConnection(dataSource.getConnection());
        H2GISFunctions.load(connection);
        pgDataSource = JDBCTestCase.getPostGISDatasourceFromEnv();
        String pgUser = Optional.ofNullable(System.getenv("POSTGRES_USER")).orElse("noisemodelling");
        String pgPass = Optional.ofNullable(System.getenv("POSTGRES_PASSWORD")).orElse("noisemodelling");
        String pgPort = Optional.ofNullable(System.getenv("POSTGRES_PORT")).orElse("5432");
        String pgDb = Optional.ofNullable(System.getenv("POSTGRES_DB")).orElse("noisemodelling_db");
        String pgHost = Optional.ofNullable(System.getenv("POSTGRES_HOST")).orElse("localhost");
        new Write_PostGIS_Settings().exec(connection, Map.of(
                "pgUser", pgUser,
                "pgPassword", pgPass,
                "pgPort", pgPort,
                "pgDatabase", pgDb,
                "pgHost", pgHost));
        assertTrue(JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION"));
        // Load the fixtures needed by the DEM exposure chain
        try (Connection pgConnection = pgDataSource.getConnection()) {
            runSqlFile(pgConnection, "database/cbs_structure.sql");
            runSqlFile(pgConnection, "database/nm_conf.sql");
            runSqlFile(pgConnection, "database/n_routier_troncon_l_hexa.sql.zip");
            runSqlFile(pgConnection, "database/n_routier_trafic_hexa.sql.zip");
            runSqlFile(pgConnection, "database/n_routier_vitesse_hexa.sql.zip");
            runSqlFile(pgConnection, "database/nm_stations_hexa.sql.zip");
            runSqlFile(pgConnection, "database/nm_link_dept_infra_road_hexa.sql.zip");
            runSqlFile(pgConnection, "database/tiny_d091.sql.zip");
            runSqlFile(pgConnection, "database/n_ligne_orographique_bdt_000_2023.sql.zip");
            runSqlFile(pgConnection, "database/n_troncon_hydrographique_bdt_000_2023.sql.zip");
            Statement statement = pgConnection.createStatement();
            // Extract DEM data from tiny wkb
            statement.execute("""
                              INSERT INTO bd_alti.d091 (the_geom)
                              SELECT d.geom
                              FROM bd_alti.tiny_d091 s
                              CROSS JOIN LATERAL (
                                SELECT (dd).geom AS geom
                                FROM ST_DumpPoints(ST_GeomFromTWKB(s.chunk_twkb)) AS dd
                              ) AS d;
                              DROP TABLE bd_alti.tiny_d091;
                              """);
            // Duplicate on 028 to check for removal of duplicate DEM points
            statement.execute("INSERT INTO bd_alti.d028 (id, the_geom) select id, the_geom from bd_alti.d091;");
        }

        // Create the emission table used to compute the extraction envelope
        ScriptUtilities.execScript(new Generate_sources(), connection, Map.of("projectionName", "hexa"));

        new ExposeDEMforUniqueUUEID().exec(connection,
                Map.of("projectionName", "hexa",
                        "uueid", "RD_FR_00_0781651",
                        "conf", 1),
                new EmptyProgressVisitor());

        // Check dem_hexa
        try (Connection pgConnection = pgDataSource.getConnection()) {
            assertTrue(JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.dem_hexa"));
            int rows = JDBCUtilities.getRowCount(pgConnection, "cbs_uge_output.dem_hexa");
            assertTrue(rows > 0, "No contour line was pushed to cbs_uge_output.dem_hexa");
            // All the contour lines belong to the processed uueid and lie on their level
            try (Statement statement = pgConnection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) CPT FROM cbs_uge_output.dem_hexa " +
                         "WHERE uueid = 'RD_FR_00_0781651' " +
                         "AND ABS(ST_ZMin(the_geom) - idiso) < 0.01 AND ABS(ST_ZMax(the_geom) - idiso) < 0.01")) {
                assertTrue(resultSet.next());
                assertEquals(rows, resultSet.getInt("CPT"));
            }
            // Contour levels are multiples of the 1 m interval
            try (Statement statement = pgConnection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) CPT FROM cbs_uge_output.dem_hexa " +
                         "WHERE ABS(idiso - ROUND(idiso)) > 0.01")) {
                assertTrue(resultSet.next());
                assertEquals(0, resultSet.getInt("CPT"));
            }
        }
    }
}
