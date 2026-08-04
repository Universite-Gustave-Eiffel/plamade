package org.noise_planet.covadis.webserver;


import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2.value.ValueBoolean;
import org.h2gis.functions.io.shp.SHPRead;
import org.h2gis.utilities.GeometryMetaData;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerUUEID;
import org.noise_planet.covadis.scripts.CBS.Generate_sources;
import org.noise_planet.covadis.scripts.CBS.Write_PostGIS_Settings;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.webserver.database.PostGISUtilities;
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test execution of the CBS scripts using a subset of the data extracted from the Plamade PostgreSQL database
 */
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
                    runSqlFile(pgConnection, "database/tiny_d091.sql.zip");
                    runSqlFile(pgConnection, "database/n_ligne_orographique_bdt_000_2023.sql.zip");
                    runSqlFile(pgConnection, "database/n_troncon_hydrographique_bdt_000_2023.sql.zip");
                    runSqlFile(pgConnection, "database/nm_nuts.sql.zip");
                    runSqlFile(pgConnection, "database/c_batimentsensible_hexa.sql");
                    runSqlFile(pgConnection, "database/n_ferroviaire_ligne.sql.zip");
                    runSqlFile(pgConnection, "database/n_ferroviaire_troncon.sql.zip");
                    // Extract DEM data from tiny wkb
                    statement.execute("""
                                      INSERT INTO bd_alti.d091 (the_geom)
                                      SELECT d.geom
                                      FROM bd_alti.tiny_d091 s
                                      CROSS JOIN LATERAL (
                                        SELECT (dd).geom AS geom
                                        FROM ST_DumpPoints(ST_GeomFromTWKB(s.chunk_twkb)) AS dd
                                      ) AS d;
                                      """);
                    // Duplicate on 028 to check for removal of duplicate DEM points
                    statement.execute("INSERT INTO bd_alti.d028 (id, the_geom) select id, the_geom from bd_alti.d091;");
                    // Set population to the nearest building from the emission road to have a result even with a low max propagation distance
                    assertEquals(1, statement.executeUpdate("update \"cbs_uge_input\".\"c_population_hexa\" set idbat = 'BAT2023130018310.20548588' where idbat = 'BAT2023130018310.1203369';"));
                    assertEquals(1, statement.executeUpdate("update \"cbs_uge_input\".\"c_batiment_s_hexa\" set nb_logts_c = 1 where idbat = 'BAT2023130018310.20548588';"));
                    // Update another building (was industrial) set it as multiple residential
                    assertEquals(1, statement.executeUpdate("update \"cbs_uge_input\".\"c_batiment_s_hexa\" set nb_logts_c = 4 where idbat = 'BAT2023130018310.1268609';"));
                    assertEquals(1, statement.executeUpdate("INSERT INTO cbs_uge_input.c_population_hexa (idpop,annee,codedept,refprod,idbat,pop_orig,pop_bat) VALUES ('POP2023130018310.1268609','2023','078','130018310','BAT2023130018310.1268609','insee iris et pmenp20 comm + ff', 12);"));
                    // Use this building BAT2023130018310.1203408 near the road as school (use the school data from BAT2023130018310.1203316 )
                    assertEquals(1, statement.executeUpdate("UPDATE cbs_uge_input.c_correspond_batiment_batimentsensible_hexa SET idbat = 'BAT2023130018310.1203408' WHERE idbat = 'BAT2023130018310.20548432';"));
                    assertEquals(1, statement.executeUpdate("UPDATE cbs_uge_input.c_correspond_batiment_batimentsensible_hexa SET geom3d = (SELECT geom3d from cbs_uge_input.c_batiment_s_hexa bh WHERE idbat = 'BAT2023130018310.1203408') WHERE idbat = 'BAT2023130018310.1203408';"));
                    logger.info("Database tables created in {}ms", System.currentTimeMillis() - start);
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
            assertEquals(20, JDBCUtilities.getRowCount(pgConnection, "cbs_uge_output.routier_trafic_hexa"));
            assertEquals(20, JDBCUtilities.getRowCount(pgConnection, "cbs_uge_output.routier_emission_hexa"));
        }
    }

    @Test
    @Order(4)
    public void testCopyPostGISToH2Database() throws SQLException {
        assumePostGISAvailable();

        try (Connection pgConnection = pgDataSource.getConnection();
             Statement statement = pgConnection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM cbs_uge_input.c_batiment_s_hexa")) {
            PostGISUtilities.copyResultSetToDatabase(pgConnection, resultSet, connection, "BUILDINGS", true, 5);
        }
        assertTrue(JDBCUtilities.tableExists(connection, "BUILDINGS"));
        assertEquals(2239, JDBCUtilities.getRowCount(connection, "BUILDINGS"));
        GeometryMetaData metaData =
                GeometryTableUtilities.getMetaData(connection, "BUILDINGS", "GEOM3D");
        assertNotNull(metaData);
        assertEquals("GEOMETRY(MULTIPOLYGONZ,2154)", metaData.getSQL());
    }

    @Test
    @Order(4)
    public void testCopyPostGISToH2DatabaseWithPk() throws SQLException {
        assumePostGISAvailable();

        try (Connection pgConnection = pgDataSource.getConnection();
             Statement statement = pgConnection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM cbs_uge_output.routier_emission_hexa")) {
            PostGISUtilities.copyResultSetToDatabase(pgConnection, resultSet, connection, "ROUTES", true, 5);
        }
        assertTrue(JDBCUtilities.tableExists(connection, "ROUTES"));
        assertEquals(20, JDBCUtilities.getRowCount(connection, "ROUTES"));
        assertEquals(1, JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse("ROUTES", DBTypes.H2)));
    }
    @Test
    @Order(5)
    public void testCopyH2ToPostGISDatabaseWithPk() throws SQLException, IOException {
        assumePostGISAvailable();
        URL url = TestCBSScript.class.getResource("buildings.shp");
        assertNotNull(url);
        SHPRead.importTable(connection, url.getFile(),"BUILDINGS" , ValueBoolean.TRUE);
        try (Connection pgConnection = pgDataSource.getConnection()) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT * FROM BUILDINGS")) {
                PostGISUtilities.copyResultSetToDatabase(connection, resultSet, pgConnection, "BUILDINGS", true, 5);
            }
            assertTrue(JDBCUtilities.tableExists(pgConnection, "BUILDINGS"));
            assertEquals(JDBCUtilities.getRowCount(connection, "BUILDINGS"), JDBCUtilities.getRowCount(pgConnection, "BUILDINGS"));
            assertEquals(1, JDBCUtilities.getIntegerPrimaryKey(pgConnection, TableLocation.parse("BUILDINGS", DBTypes.POSTGIS)));
        }
    }
    @Test
    @Order(5)
    public void testRunByUUEID() throws SQLException {
        assumePostGISAvailable();
        //RD_FR_00_0781651
        ScriptUtilities.execScript(new ComputePerUUEID(), connection, Map.of(
                "projectionName", "hexa",
                "uueid_pattern", "RD_FR_00_0781651",
                "conf", 1));



        // Check cbs
        List<String> expectedCbs = Arrays.asList("Lden5559", "Lden6064", "Lden6569", "Lden7074", "LdenGreaterThan62",
                "LdenGreaterThan68", "LdenGreaterThan75", "Lnight5054", "Lnight5559", "Lnight6064", "Lnight6569", "LnightGreaterThan70");
        try(Connection pgConnection = pgDataSource.getConnection();
            Statement statement = pgConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT distinct noiselevel FROM \"cbs_uge_output\".\"cbs_hexa\" order by noiselevel;")) {
            for (String expectedCb : expectedCbs) {
                assertTrue(resultSet.next());
                assertEquals(expectedCb, resultSet.getString("noiselevel"));
            }
        }

        try(Connection pgConnection = pgDataSource.getConnection();
            Statement statement = pgConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) CPT FROM cbs_uge_output.facade_expo_hexa")) {
            assertTrue(resultSet.next());
            assertEquals(56, resultSet.getInt("CPT"));
        }

        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT schools from expo_hexa where pk = 'RD_FR_00_0781651_Lnight5559';")) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt("schools"));
        }

        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) CPT FROM DEM")) {
            assertTrue(resultSet.next());
            assertEquals(35178, resultSet.getInt("CPT"));
        }
    }

}
