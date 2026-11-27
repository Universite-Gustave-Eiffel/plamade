/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and
 * education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.webserver.utilities;

import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.noise_planet.covadis.scripts.CBS.ComputePerDepartment;
import org.noise_planet.covadis.scripts.JDBCTestCase;
import org.noise_planet.covadis.scripts.Slurm.ReceiversQueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class ScriptUtilitiesTest extends JDBCTestCase {

    Logger logger = LoggerFactory.getLogger(ScriptUtilitiesTest.class);

    @Test
    public void testFillDefaultValuesWithUnexpectedInputThrows() {
        Map inputs = new HashMap();
        inputs.put("projectionName", "hexa");
        inputs.put("unknownInput", "value");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ScriptUtilities.fillDefaultValues(ComputePerDepartment.class, inputs));
        assertTrue(ex.getMessage().contains("unknownInput"));
    }

    @Test
    public void testFillDefaultValuesWithDeclaredInputsSucceeds() {
        Map inputs = new HashMap();
        inputs.put("projectionName", "hexa");

        Map result = ScriptUtilities.fillDefaultValues(ComputePerDepartment.class, inputs);
        assertEquals("hexa", result.get("projectionName"));
        assertTrue(result.containsKey("projectionName"));
    }

    @Test
    public void testWriteResultSetAsSqlH2RoundTrip() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE SRC_DATA (id INT PRIMARY KEY, name VARCHAR(50), " +
                    "val NUMERIC(10, 4), note VARCHAR(50), flag BOOLEAN, geom GEOMETRY(POINTZ, 2154))");
            stmt.execute("INSERT INTO SRC_DATA VALUES (1, 'abc', 123.4567, 'O''Reilly', TRUE, " +
                    "ST_GeomFromText('POINT Z (1 2 3)', 2154))");
            stmt.execute("INSERT INTO SRC_DATA VALUES (2, NULL, NULL, 'plain', FALSE, NULL)");
        }

        String sql;
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery("SELECT * FROM SRC_DATA");
             StringWriter writer = new StringWriter()) {
            ScriptUtilities.writeResultSetAsSql(connection, resultSet, "DST_DATA", writer, DBTypes.H2GIS, true);
            sql = writer.toString();
        }

        assertTrue(sql.contains("CREATE TABLE DST_DATA"));
        assertTrue(sql.contains("O''Reilly"));
        assertTrue(sql.contains("123.4567"));

        executeScript(connection, sql);

        assertEquals(2, JDBCUtilities.getRowCount(connection, "DST_DATA"));
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM DST_DATA ORDER BY ID")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ID"));
            assertEquals("abc", rs.getString("NAME"));
            assertEquals(0, new BigDecimal("123.4567").compareTo(rs.getBigDecimal("VAL")));
            assertEquals("O'Reilly", rs.getString("NOTE"));
            assertTrue(rs.getBoolean("FLAG"));
            Geometry geom = (Geometry) rs.getObject("GEOM");
            assertNotNull(geom);
            assertEquals(1.0, geom.getCoordinate().x);
            assertEquals(2.0, geom.getCoordinate().y);
            assertEquals(3.0, geom.getCoordinate().getZ());
            assertEquals(2154, geom.getSRID());

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("ID"));
            assertNull(rs.getString("NAME"));
            assertTrue(rs.wasNull());
            assertNull(rs.getBigDecimal("VAL"));
            assertTrue(rs.wasNull());
            assertFalse(rs.getBoolean("FLAG"));
            assertNull(rs.getObject("GEOM"));
        }
    }

    @Test
    public void testWriteResultSetAsSqlRunScriptImport() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE SRC_DATA (id INT PRIMARY KEY, val NUMERIC(10, 4), " +
                    "geom GEOMETRY(POINTZ, 2154))");
            stmt.execute("INSERT INTO SRC_DATA VALUES (1, 123.4567, " +
                    "ST_GeomFromText('POINT Z (1 2 3)', 2154))");
        }

        // Write the SQL script compressed with gzip, as done by the Slurm input export
        File sqlFile = File.createTempFile("writeResultSetAsSql", ".sql.gz");
        sqlFile.deleteOnExit();
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery("SELECT * FROM SRC_DATA");
             OutputStream os = new GZIPOutputStream(new FileOutputStream(sqlFile));
             Writer writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            ScriptUtilities.writeResultSetAsSql(connection, resultSet, "DST_DATA", writer, DBTypes.H2GIS, true);
        }

        // Import the script with RUNSCRIPT, the path used to load the merged output data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS DST_DATA");
            stmt.execute("RUNSCRIPT FROM '" + sqlFile.getAbsolutePath() + "' COMPRESSION GZIP");
        }

        assertEquals(1, JDBCUtilities.getRowCount(connection, "DST_DATA"));
        assertEquals(1, JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse("DST_DATA", DBTypes.H2)));
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM DST_DATA")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ID"));
            assertEquals(0, new BigDecimal("123.4567").compareTo(rs.getBigDecimal("VAL")));
            Geometry geom = (Geometry) rs.getObject("GEOM");
            assertNotNull(geom);
            assertEquals(2154, geom.getSRID());
        }
    }

    private void executeScript(Connection connection, String script) throws Exception {
        ScriptReader scriptReader = new ScriptReader(new StringReader(script));
        scriptReader.setSkipRemarks(true);
        String statement = scriptReader.readStatement();
        try (Statement stmt = connection.createStatement()) {
            while (statement != null && !StringUtils.isWhitespaceOrEmpty(statement)) {
                stmt.execute(statement);
                statement = scriptReader.readStatement();
            }
        }
    }

    @Test
    public void testParseDumpScript() throws SQLException, IOException {
        URL resource = ScriptUtilitiesTest.class.getResource("sql_dump.sql.gz");
        assertNotNull(resource);
        FileUtilities.runSqlDump(Collections.singletonList(resource.getPath()), connection, new EmptyProgressVisitor(),
                false, logger, new ReceiversQueryFilter());

        // Check if the table exists
        assertTrue(JDBCUtilities.tableExists(connection, "RECEIVERS_LEVEL"));
        assertEquals(39, JDBCUtilities.getRowCount(connection, "RECEIVERS_LEVEL"));

    }
}
