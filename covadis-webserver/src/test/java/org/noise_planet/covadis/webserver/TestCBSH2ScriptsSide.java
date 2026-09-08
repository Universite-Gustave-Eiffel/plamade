package org.noise_planet.covadis.webserver;

import groovy.sql.Sql;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerUUEID;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class TestCBSH2ScriptsSide extends JDBCTestCase {

    @Test
    public void testGenerateExposureStatisticsFromFacadeExpo() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("RUNSCRIPT FROM '" + Objects.requireNonNull(TestCBSH2ScriptsSide.class.getResource("testGenerateExposureStatisticsFromFacadeExpo.sql")).getFile() + "'");
        }

        ComputePerUUEID.generateExposureStatisticsFromFacadeExpo(connection, "RD_FR_00_0781651", Map.of("078", "FR103"), "hexa");

        // Check peoples on single dwelling
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lden6064'");) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("PEOPLE"));
            assertEquals(1, rs.getInt("DWELLINGS"));
        }
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lnight5559'");) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("PEOPLE"));
            assertEquals(1, rs.getInt("DWELLINGS"));
        }
        // I set a building with 65 peoples in 32 dwellings all exposed at night at the level 53 dB
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lnight5054'");) {
            assertTrue(rs.next());
            assertEquals(65, rs.getInt("PEOPLE"));
            assertEquals(32, rs.getInt("DWELLINGS"));
        }
        // Hospital exposed to RD_FR_00_0781651_Lden5559
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT HOSPITALS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lden5559'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("HOSPITALS"));
        }
        // Schools exposed to RD_FR_00_0781651_Lden6064
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT SCHOOLS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lden6064'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("SCHOOLS"));
        }
        // Hospital exposed to RD_FR_00_0781651_Lnight5054
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT HOSPITALS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lnight5054'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("HOSPITALS"));
        }
        // Schools exposed to RD_FR_00_0781651_Lnight5559
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT SCHOOLS FROM EXPO_hexa where pk='RD_FR_00_0781651_Lnight5559'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("SCHOOLS"));
        }
    }


    @Test
    public void testComputeCardiacIschemiaHsdHa() throws SQLException {
        // Compare expected computed results

        try(Statement stmt = connection.createStatement()) {
            stmt.execute("RUNSCRIPT FROM '" + Objects.requireNonNull(TestCBSH2ScriptsSide.class.getResource("testGenerateHealthStatistics.sql")).getFile() + "'");
        }

        ComputePerUUEID.generateHealthStatistics(connection, "hexa");

        LoggerFactory.getLogger(TestCBSH2ScriptsSide.class).info(
                ScriptUtilities.formatSqlQueryResult(new Sql(connection), "SELECT * FROM EXPO_HEXA", 120));
        LoggerFactory.getLogger(TestCBSH2ScriptsSide.class).info(
                ScriptUtilities.formatSqlQueryResult(new Sql(connection), "SELECT * FROM EXPO_GLOBAL_HEXA", 120));
        // table EXPO_GLOBAL_HEXA expected values
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CPI, HA, HSD FROM EXPO_GLOBAL_HEXA")) {
            assertTrue(rs.next());
            // Expected value extracted from the Acoucité computation sheet
            assertEquals(30.3107871703154, rs.getFloat("CPI"), 0.01);
            assertEquals(57482.775, rs.getFloat("HA"), 0.01);
            assertEquals(13454.849, rs.getFloat("HSD"), 0.01);
            assertFalse(rs.next());
        }
    }
}

