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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class ScriptUtilitiesTest extends JDBCTestCase {
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

    @Test
    public void testMergeSqlFilesKeepsConstraintsOnce() throws Exception {
        // The remote noisemodelling container exports RECEIVERS_LEVEL with H2 SCRIPT whose
        // data section is made of batched multi-line INSERT statements and whose tail
        // contains "ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY" and "CREATE INDEX"
        // statements, one set per task file. Only the first file must keep them; later files
        // only contribute the data rows, otherwise RUNSCRIPT fails with
        // 'Constraint "CONSTRAINT_B" already exists'.
        String tableHeader =
                "-- H2 2.5.252;\n" +
                        ";\n" +
                        "CREATE USER IF NOT EXISTS \"SA\" PASSWORD '' ADMIN;\n" +
                        "CREATE CACHED TABLE \"PUBLIC\".\"RECEIVERS_LEVEL\"(\n" +
                        "    \"IDRECEIVER\" BIGINT NOT NULL,\n" +
                        "    \"PERIOD\" CHARACTER VARYING NOT NULL,\n" +
                        "    \"THE_GEOM\" GEOMETRY(POINT Z, 2154),\n" +
                        "    \"LAEQ\" REAL NOT NULL\n" +
                        ");\n" +
                        "-- 1 +/- SELECT COUNT(*) FROM PUBLIC.RECEIVERS_LEVEL;\n";
        String trailing =
                "ALTER TABLE \"PUBLIC\".\"RECEIVERS_LEVEL\" ADD CONSTRAINT \"PUBLIC\".\"CONSTRAINT_B\" PRIMARY KEY(\"IDRECEIVER\", \"PERIOD\");\n" +
                        "CREATE INDEX \"PUBLIC\".\"RECEIVERS_LEVEL_IDX\" ON \"PUBLIC\".\"RECEIVERS_LEVEL\"(\"IDRECEIVER\", \"PERIOD\");\n";
        File firstFile = writeGzipSql(File.createTempFile("receivers_level_1", ".sql.gz"),
                tableHeader +
                        "INSERT INTO \"PUBLIC\".\"RECEIVERS_LEVEL\" VALUES\n" +
                        "(1, 'DEN', GEOMETRY X'00a00000010000086a410b483fe7821e1f4159c6cbff4be9220000000000000000', 60.0);\n" +
                        trailing);
        File secondFile = writeGzipSql(File.createTempFile("receivers_level_2", ".sql.gz"),
                tableHeader +
                        "INSERT INTO \"PUBLIC\".\"RECEIVERS_LEVEL\" VALUES\n" +
                        "(2, 'N', GEOMETRY X'00a00000010000086a410b483fe7821e1f4159c6cbff4be9220000000000000000', 50.0),\n" +
                        "(3, 'DEN', GEOMETRY X'00a00000010000086a410b483fe7821e1f4159c6cbff4be9220000000000000000', 45.0);\n" +
                        "INSERT INTO \"PUBLIC\".\"RECEIVERS_LEVEL\" VALUES\n" +
                        "(4, 'N', GEOMETRY X'00a00000010000086a410b483fe7821e1f4159c6cbff4be9220000000000000000', 40.0);\n" +
                        trailing);

        File mergedFile = File.createTempFile("RECEIVERS_LEVEL", ".sql.gz");
        mergedFile.deleteOnExit();
        FileUtilities.mergeSqlFiles(List.of(firstFile.getPath(), secondFile.getPath()), mergedFile, new EmptyProgressVisitor());

        // Constraints and indexes must be present only once in the merged script
        String mergedSql;
        try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(mergedFile))) {
            mergedSql = new String(gzis.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertEquals(1, countOccurrences(mergedSql, "ADD CONSTRAINT"));
        assertEquals(1, countOccurrences(mergedSql, "CREATE INDEX"));

        // The merged script must load without "constraint already exists" and keep all rows
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS RECEIVERS_LEVEL");
            stmt.execute("RUNSCRIPT FROM '" + mergedFile.getAbsolutePath() + "' COMPRESSION GZIP");
        }
        assertEquals(4, JDBCUtilities.getRowCount(connection, "RECEIVERS_LEVEL"));
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT IDRECEIVER FROM RECEIVERS_LEVEL ORDER BY IDRECEIVER")) {
            for (int expected : new int[]{1, 2, 3, 4}) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getInt(1));
            }
        }
    }

    private static File writeGzipSql(File file, String sql) throws Exception {
        file.deleteOnExit();
        try (OutputStream os = new GZIPOutputStream(new FileOutputStream(file))) {
            os.write(sql.getBytes(StandardCharsets.UTF_8));
        }
        return file;
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int index = 0;
        while ((index = haystack.indexOf(needle, index)) != -1) {
            count++;
            index += needle.length();
        }
        return count;
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
}
