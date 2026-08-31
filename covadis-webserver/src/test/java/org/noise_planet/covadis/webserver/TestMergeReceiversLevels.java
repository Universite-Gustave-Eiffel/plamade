package org.noise_planet.covadis.webserver;

import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerDept;
import org.noise_planet.covadis.scripts.JDBCTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for ComputePerDept.mergeReceiversLevels: energetic sum of receiver levels
 * across pos_sol tables, without losing contributions (NULL injection) nor dropping
 * receiver periods present on a single side.
 */
public class TestMergeReceiversLevels extends JDBCTestCase {

    @Test
    public void testMergePosSols() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE ROADS_LEVELS_0 (THE_GEOM GEOMETRY(POINT Z, 2154), IDRECEIVER INTEGER, PERIOD VARCHAR, LAEQ NUMERIC(5,2) NOT NULL)");
            stmt.execute("CREATE TABLE ROADS_LEVELS_1 (THE_GEOM GEOMETRY(POINT Z, 2154), IDRECEIVER INTEGER, PERIOD VARCHAR, LAEQ NUMERIC(5,2) NOT NULL)");

            // pos_sol = 1 (base table, pop() takes the last pos_sol of the list)
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (1 2 4)', 2154), 1, 'DEN', 60.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (1 2 4)', 2154), 1, 'N', 50.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (2 2 4)', 2154), 2, 'DEN', 55.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (2 2 4)', 2154), 2, 'N', 45.0)");
            // receiver 5: pos_sol=0 has only the DEN period -> N must not become NULL
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (5 2 4)', 2154), 5, 'DEN', 40.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (5 2 4)', 2154), 5, 'N', 30.0)");
            // receiver 6: no N period in pos_sol=1 -> the N period of pos_sol=0 must be inserted
            stmt.execute("INSERT INTO ROADS_LEVELS_1 VALUES (ST_GeomFromText('POINT Z (6 2 4)', 2154), 6, 'DEN', 35.0)");

            // pos_sol = 0 (merged into the base table)
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (1 2 4)', 2154), 1, 'DEN', 60.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (1 2 4)', 2154), 1, 'N', 50.0)");
            // receiver 3: only in pos_sol=0 -> inserted as is
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (3 2 4)', 2154), 3, 'DEN', 52.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (3 2 4)', 2154), 3, 'N', 42.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (5 2 4)', 2154), 5, 'DEN', 40.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (6 2 4)', 2154), 6, 'DEN', 35.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (6 2 4)', 2154), 6, 'N', 28.0)");
            stmt.execute("INSERT INTO ROADS_LEVELS_0 VALUES (ST_GeomFromText('POINT Z (7 2 4)', 2154), 7, 'DEN', 50.0)");
        }

        ComputePerDept.mergeReceiversLevels(List.of("0", "1"), connection, "TEST", null, null);

        // receiver in both pos_sol and both periods: energetic sum 60+60 and 50+50
        assertEquals(63.01, getLaeq(1, "DEN"), 0.01);
        assertEquals(53.01, getLaeq(1, "N"), 0.01);
        // receiver only in pos_sol=1: unchanged
        assertEquals(55.00, getLaeq(2, "DEN"), 0.01);
        assertEquals(45.00, getLaeq(2, "N"), 0.01);
        // receiver only in pos_sol=0: inserted as is
        assertEquals(52.00, getLaeq(3, "DEN"), 0.01);
        assertEquals(42.00, getLaeq(3, "N"), 0.01);
        // receiver 5: DEN summed (40+40), N keeps the pos_sol=1 value (no NULL injection)
        assertEquals(43.01, getLaeq(5, "DEN"), 0.01);
        assertEquals(30.00, getLaeq(5, "N"), 0.01);
        // receiver 6: DEN summed (35+35), N only present in pos_sol=0 -> inserted
        assertEquals(38.01, getLaeq(6, "DEN"), 0.01);
        assertEquals(28.00, getLaeq(6, "N"), 0.01);
        // receiver 7: only DEN, only in pos_sol=0
        assertEquals(50.00, getLaeq(7, "DEN"), 0.01);

        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM RECEIVERS_LEVEL_TEST")) {
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
        }
        // no N period row for receiver 7
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM RECEIVERS_LEVEL_TEST WHERE IDRECEIVER = 7 AND PERIOD = 'N'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
        // no NULL levels anywhere
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM RECEIVERS_LEVEL_TEST WHERE LAEQ IS NULL")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    private double getLaeq(int idReceiver, String period) throws SQLException {
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT LAEQ FROM RECEIVERS_LEVEL_TEST WHERE IDRECEIVER = " + idReceiver + " AND PERIOD = '" + period + "'")) {
            assertTrue(rs.next(), "No row for receiver " + idReceiver + " period " + period);
            return rs.getDouble("LAEQ");
        }
    }
}
