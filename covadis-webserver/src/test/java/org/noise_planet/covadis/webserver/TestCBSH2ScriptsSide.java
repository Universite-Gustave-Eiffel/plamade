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

        ComputePerUUEID.generateExposureStatisticsFromFacadeExpo(connection);

        // Check peoples on single dwelling
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO where noiselevel='Lden6064'");) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("PEOPLE"));
            assertEquals(1, rs.getInt("DWELLINGS"));
        }
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO where noiselevel='Lnight5559'");) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("PEOPLE"));
            assertEquals(1, rs.getInt("DWELLINGS"));
        }
        // I set a building with 65 peoples in 32 dwellings all exposed at night at the level 53 dB
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT PEOPLE, DWELLINGS FROM EXPO where noiselevel='Lnight5054'");) {
            assertTrue(rs.next());
            assertEquals(65, rs.getInt("PEOPLE"));
            assertEquals(32, rs.getInt("DWELLINGS"));
        }
        // Hospital exposed to Lden5559
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT HOSPITALS FROM EXPO where noiselevel='Lden5559'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("HOSPITALS"));
        }
        // Schools exposed to Lden6064
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT SCHOOLS FROM EXPO where noiselevel='Lden6064'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("SCHOOLS"));
        }
        // Hospital exposed to Lnight5054
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT HOSPITALS FROM EXPO where noiselevel='Lnight5054'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("HOSPITALS"));
        }
        // Schools exposed to Lnight5559
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT SCHOOLS FROM EXPO where noiselevel='Lnight5559'");) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("SCHOOLS"));
        }

        //EXPO_GLOBAL
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT HA, HSD, CPI FROM EXPO_GLOBAL");) {
            assertTrue(rs.next());
            assertEquals(12.88, rs.getDouble("HA"), 0.01);
            assertEquals(3.76, rs.getDouble("HSD"), 0.01);
            assertEquals(0.00376, rs.getDouble("CPI"), 0.00001);
        }


    }


    @Test
    public void testComputeCardiacIschemiaHsdHa() throws SQLException {
        // Compare expected computed results

        try(Statement stmt = connection.createStatement()) {
            stmt.execute("RUNSCRIPT FROM '" + Objects.requireNonNull(TestCBSH2ScriptsSide.class.getResource("testGenerateHealthStatistics.sql")).getFile() + "'");
        }

        ComputePerUUEID.generateHealthStatistics(connection);

        LoggerFactory.getLogger(TestCBSH2ScriptsSide.class).info(
                ScriptUtilities.formatSqlQueryResult(new Sql(connection), "SELECT * FROM EXPOSURE_RANGES", 120));
        LoggerFactory.getLogger(TestCBSH2ScriptsSide.class).info(
                ScriptUtilities.formatSqlQueryResult(new Sql(connection), "SELECT * FROM EXPO_GLOBAL", 120));
        // table EXPO_GLOBAL expected values
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CPI, HA, HSD FROM EXPO_GLOBAL")) {
            assertTrue(rs.next());
            // Expected value extracted from the Acoucité computation sheet
            assertEquals(30.3107871703154, rs.getFloat("CPI"), 0.01);
            assertEquals(57482.775, rs.getFloat("HA"), 0.01);
            assertEquals(13454.849, rs.getFloat("HSD"), 0.01);
            assertFalse(rs.next());
        }
    }
}

