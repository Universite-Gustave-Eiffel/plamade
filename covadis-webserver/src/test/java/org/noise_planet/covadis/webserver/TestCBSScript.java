package org.noise_planet.covadis.webserver;


import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.Generate_sources;
import org.noise_planet.covadis.scripts.CBS.Write_PostGIS_Settings;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities;
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCBSScript extends JDBCTestCase {
    DataSource pgDataSource;
    boolean forceRecreateData = true;


    @BeforeEach
    public void writePgConfigurationInH2MemDb() throws SQLException {
        assumePostGISAvailable();
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
    }

    private static void assumePostGISAvailable() {
        String pgHost = System.getenv("POSTGRES_HOST");
        Assumptions.assumeTrue(pgHost != null && !pgHost.isEmpty(), "POSTGRES_HOST is not defined, skipping PostGIS test");
    }

    private static void runSqlFile(Connection pgConnection, String sqlPath) throws IOException {
        URL scriptUrl = TestCBSScript.class.getResource(sqlPath);
        // Read content
        String file = scriptUrl.getFile();
        if(file == null) {
            throw new IOException("File not found: " + sqlPath);
        }
        if(file.endsWith(".zip"))
        {
            // Handle ZIP file
            try(ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(file))) {
                ZipEntry entry = zipInputStream.getNextEntry();
                while (entry != null) {
                    // Process sql file
                    if(entry.getName().endsWith(".sql")) {
                        String sql = new String(zipInputStream.readAllBytes());
                        new Execute_Query().exec(pgConnection,
                                Map.of("sqlQueries", sql, "outputFormat", "json"),
                                new EmptyProgressVisitor());
                    }
                    entry = zipInputStream.getNextEntry();
                }
            }
        } else {
            try (FileReader fileReader = new FileReader(file)){
                String sql = fileReader.readAllAsString();
                new Execute_Query().exec(pgConnection,
                        Map.of("sqlQueries", sql, "outputFormat", "json"),
                        new EmptyProgressVisitor());
            }
        }
    }

    @Test
    @Order(1)
    public void initDb() throws SQLException, IOException {
        assumePostGISAvailable();
        // Initialize database for non-H2GIS databases
        pgDataSource = getPostGISDatasourceFromEnv();
        try (Connection pgConnection = pgDataSource.getConnection()) {
            // Check if the schema cbs_uge_output exists:
            Statement statement = pgConnection.createStatement();
            try (ResultSet rs = statement.executeQuery("SELECT * FROM information_schema.schemata WHERE schema_name = 'cbs_uge_input'")) {
                if (forceRecreateData || !rs.next()) {
                    // Schema does not exist
                    runSqlFile(pgConnection, "database/cbs_structure.sql");
                    runSqlFile(pgConnection, "database/nm_conf.sql");
                    runSqlFile(pgConnection, "database/c_batiment_s_hexa.sql.zip");

                }
            }
        }
    }

    @Test
    @Order(3)
    public void testGenerateSource() {
        assumePostGISAvailable();
        ScriptUtilities.execScript(new Generate_sources(), connection, Map.of("projectionName", "hexa"));
    }
}
