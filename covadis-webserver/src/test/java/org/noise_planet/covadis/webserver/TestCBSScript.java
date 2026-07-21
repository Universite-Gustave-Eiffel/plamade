package org.noise_planet.covadis.webserver;


import groovy.sql.Sql;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerUUEID;
import org.noise_planet.covadis.scripts.CBS.Generate_sources;
import org.noise_planet.covadis.scripts.CBS.Write_PostGIS_Settings;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities;
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query;
import org.noise_planet.noisemodelling.scripts.Import_and_Export.Export_Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TestCBSScript extends JDBCTestCase {
    DataSource pgDataSource;
    boolean forceRecreateData = true;
    Logger logger = LoggerFactory.getLogger(TestCBSScript.class);


    @BeforeEach
    public void writePgConfigurationInH2MemDb() throws SQLException {
        assumePostGISAvailable();
        pgDataSource = getPostGISDatasourceFromEnv();
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

    private static void runSqlFile(Connection pgConnection, String sqlPath) throws IOException, SQLException {
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
                    if(!entry.isDirectory()) {
                        String sql = new String(zipInputStream.readAllBytes());
                        executeScript(pgConnection, sql);
                    }
                    entry = zipInputStream.getNextEntry();
                }
            }
        } else {
            try (FileReader fileReader = new FileReader(file)){
                String sql = fileReader.readAllAsString();
                executeScript(pgConnection, sql);
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
    @Order(1)
    public void initDb() throws SQLException, IOException {
        assumePostGISAvailable();
        try (Connection pgConnection = pgDataSource.getConnection()) {
            // Check if the schema cbs_uge_output exists:
            Statement statement = pgConnection.createStatement();
            try (ResultSet rs = statement.executeQuery("SELECT * FROM information_schema.schemata WHERE schema_name = 'cbs_uge_input'")) {
                if (forceRecreateData || !rs.next()) {
                    // Schema does not exist
                    logger.info("Creating database tables...");
                    long start = System.currentTimeMillis();
                    runSqlFile(pgConnection, "database/cbs_structure.sql");
                    runSqlFile(pgConnection, "database/nm_conf.sql");
                    runSqlFile(pgConnection, "database/c_batiment_s_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/n_routier_troncon_l_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/n_routier_trafic_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/n_routier_vitesse_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/nm_stations_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/c_population_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/c_correspond_batiment_batimentsensible_hexa.sql");
                    runSqlFile(pgConnection, "database/n_routier_protection_acoustique_hexa.sql");
                    runSqlFile(pgConnection, "database/c_naturesol_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/nm_link_dept_infra_road_hexa.sql.zip");
                    runSqlFile(pgConnection, "database/d028.sql.zip");
                    runSqlFile(pgConnection, "database/d078.sql.zip");
                    runSqlFile(pgConnection, "database/d091.sql.zip");
                    runSqlFile(pgConnection, "database/n_ligne_orographique_bdt_000_2023.sql.zip");
                    runSqlFile(pgConnection, "database/n_troncon_hydrographique_bdt_000_2023.sql.zip");
                    logger.info("Database tables created in " + (System.currentTimeMillis() - start) + "ms");
                }
            }
        }
    }

    @Test
    @Order(3)
    public void testGenerateSource() throws SQLException {
        assumePostGISAvailable();
        ScriptUtilities.execScript(new Generate_sources(), connection, Map.of("projectionName", "hexa"));
        try(Connection pgConnection = pgDataSource.getConnection()) {
            assertEquals(12, JDBCUtilities.getRowCount(pgConnection, "cbs_uge_output.routier_trafic_hexa"));
            assertEquals(12, JDBCUtilities.getRowCount(pgConnection, "cbs_uge_output.routier_emission_hexa"));
        }
    }

    @Test
    @Order(4)
    public void testRunByUUEID() throws SQLException {
        assumePostGISAvailable();
        //RD_FR_00_0781651
        ScriptUtilities.execScript(new ComputePerUUEID(), connection, Map.of(
                "projectionName", "hexa",
                "uueid_pattern", "RD_FR_00_0781651",
                "conf", 1));

        logger.info(ScriptUtilities.formatSqlQueryResult(
                new Sql(connection), "SELECT * FROM ISOPHONES LIMIT 5", 120));

        logger.info(ScriptUtilities.formatSqlQueryResult(
                new Sql(connection), "SELECT DISTINCT ISOLABEL FROM CONTOURING_NOISE_MAP", 120));
    }
}
