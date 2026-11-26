package org.noise_planet.covadis.webserver.utilities;

import groovy.lang.MetaMethod;
import groovy.lang.Script;
import groovy.sql.Sql;
import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper;

import org.h2.util.geometry.EWKTUtils;
import org.h2.util.geometry.JTSUtils;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.Tuple;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.locationtech.jts.geom.Geometry;
import org.noise_planet.covadis.webserver.database.PostGISUtilities;
import org.noise_planet.noisemodelling.webserver.script.ExecutionPlan;
import org.noise_planet.noisemodelling.webserver.script.ScriptMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ScriptUtilities {

    public static Object execScript(Script script, Connection connection, Map inputs) {
        return execScript(script, connection, inputs, new EmptyProgressVisitor());
    }

    public static Object execScript(Script script, Connection connection, Map inputs, ProgressVisitor progress) {
        inputs = ScriptUtilities.fillDefaultValues(script.getClass(), inputs);

        // Check expected arguments
        List<MetaMethod> methods = script.getMetaClass().getMethods();
        MetaMethod execMetaMethod = null;
        // Take the exec method with the most arguments, in case there are multiple exec methods,
        // we want to use the one with the most arguments to provide more features to the script author
        for (MetaMethod method : methods) {
            if (method.getName().equals("exec")) {
                if (execMetaMethod == null || method.getNativeParameterTypes().length > execMetaMethod.getNativeParameterTypes().length) {
                    execMetaMethod = method;
                }
            }
        }
        boolean useProgressVisitor = false; // third argument is a ProgressVisitor
        if (execMetaMethod != null) {
            // 2. Access the native parameter types
            Class[] parameterTypes = execMetaMethod.getNativeParameterTypes();
            if (parameterTypes.length >= 3 && parameterTypes[2].equals(ProgressVisitor.class)) {
                useProgressVisitor = true;
            }
        }
        if(useProgressVisitor) {
            return script.invokeMethod("exec", new Object[] { connection, inputs,
                     progress});
        }
        return script.invokeMethod("exec", new Object[] {connection, inputs});
    }
    /**
     * Read class metadata and fill with default values specified in the script
     * @param groovyClass Groovy class to extract metadata
     * @param inputs Parameters of the exec method
     * @return Filled inputs
     * @throws IllegalArgumentException Unexpected input(s) for script
     */
    public static Map fillDefaultValues(Class groovyClass, Map inputs) {
        Map filledInputs = new HashMap(inputs);
        ConfigObject config = new ConfigSlurper().parse(groovyClass);
        ((Map<String, Map>) config.get("inputs")).entrySet( ).stream().filter(
                        entry -> entry.getValue().containsKey("default")
                                && !inputs.containsKey(entry.getKey()))
                .forEach(entry -> {
                    Object defaultValue = entry.getValue().get("default");
                    Class<?> expectedType = (Class<?>) entry.getValue().get("type");
                    // Groovy may generate BigDecimal instead of expected class
                    // So cast/convert to the expected type
                    if(expectedType != null && !expectedType.isAssignableFrom(defaultValue.getClass())) {
                        try {
                            defaultValue = ScriptMetadata.castInputUsingExpectedInputType(expectedType, defaultValue.toString());
                        } catch (Exception ex) {
                            Logger logger = LoggerFactory.getLogger(ExecutionPlan.class);
                            logger.info("Warning, failed to cast default value for input '{}', use the original value. Exception: {}",
                                    entry.getKey(), ex.getMessage());
                        }
                    }
                    filledInputs.put(entry.getKey(), defaultValue);
                });
        // Check if there is more inputs key than expected by the groovy class
        Set<String> expectedInputs = ((Map<String, Map>) config.get("inputs")).keySet();
        Set<String> unexpectedInputs = new LinkedHashSet<>(filledInputs.keySet());
        unexpectedInputs.removeAll(expectedInputs);
        if (!unexpectedInputs.isEmpty()) {
            throw new IllegalArgumentException("Unexpected input(s) for script " + groovyClass.getSimpleName()
                    + ": " + unexpectedInputs + ". Declared inputs are: " + expectedInputs);
        }

        return filledInputs;
    }

    /**
     * Executes a SQL query and return the result set as a Markdown table.
     *
     * @param sql         An instance of groovy.sql.Sql
     * @param query       A String or GString
     * @param maxColWidth Maximum width of a column before truncation
     */
    public static String formatSqlQueryResult(Sql sql, Object query, int maxColWidth) throws SQLException {
        List<Map<String, Object>> rawRows = new ArrayList<>();
        try {
            List<?> rows = sql.rows(query.toString());
            for (Object row : rows) {
                if (row instanceof Map) {
                    rawRows.add((Map<String, Object>) row);
                }
            }
        } catch (SQLException e) {
            PostGISUtilities.rethrowException(e, query.toString());
        }

        if (rawRows.isEmpty()) {
            return String.format("Query returned 0 rows.\n\nSQL: `%s`", query);
        }

        List<String> columnNames = new ArrayList<>(rawRows.get(0).keySet());

        // 1. Pre-format all data
        List<Map<String, String>> formattedRows = new ArrayList<>();
        for (Map<String, Object> row : rawRows) {
            Map<String, String> formattedRow = new LinkedHashMap<>();
            for (String col : columnNames) {
                Object val = row.get(col);
                String formattedVal;

                if (val == null) {
                    formattedVal = "null";
                } else if (val instanceof java.sql.Array) {
                    // Handle H2 Arrays: strip "ARRAY [CAST...]" and format as [1.2, 3.4]
                    try {
                        Object[] arr = (Object[]) ((java.sql.Array) val).getArray();
                        List<String> elements = new ArrayList<>();
                        for (Object o : arr) {
                            if (o instanceof Number) {
                                elements.add(String.format(Locale.US, "%.2f", ((Number) o).doubleValue()));
                            } else {
                                elements.add(o.toString());
                            }
                        }
                        formattedVal = "[" + String.join(", ", elements) + "]";
                    } catch (Exception e) {
                        formattedVal = val.toString();
                    }
                } else if (val instanceof Number && !(val instanceof Integer || val instanceof Long)) {
                    formattedVal = String.format(Locale.US, "%.2f", ((Number) val).doubleValue());
                } else if (val instanceof Geometry) {
                    // Format to EWKT
                    formattedVal = EWKTUtils.ewkb2ewkt(JTSUtils.geometry2ewkb((Geometry) val));
                } else {
                    formattedVal = val.toString().replace("\n", " "); // Markdown tables must be single line
                }

                // Truncate if necessary
                if (formattedVal.length() > maxColWidth) {
                    formattedVal = formattedVal.substring(0, maxColWidth - 3) + "...";
                }
                formattedRow.put(col, formattedVal);
            }
            formattedRows.add(formattedRow);
        }

        // 2. Calculate column widths
        Map<String, Integer> columnWidths = new HashMap<>();
        for (String col : columnNames) {
            int maxLen = col.length();
            for (Map<String, String> row : formattedRows) {
                maxLen = Math.max(maxLen, row.get(col).length());
            }
            columnWidths.put(col, Math.max(3, maxLen)); // Markdown needs at least 3 chars for separators
        }

        // 3. Build Markdown Table
        StringBuilder table = new StringBuilder();
        table.append("\nSQL: `").append(query).append("`\n\n");

        // Header
        table.append("|");
        for (String col : columnNames) {
            int width = columnWidths.get(col);
            table.append(String.format(" %-" + width + "s |", col));
        }
        table.append("\n");

        // Markdown Separator Row (| --- | --- |)
        table.append("|");
        for (String col : columnNames) {
            int width = columnWidths.get(col);
            table.append(" ").append("-".repeat(width)).append(" |");
        }
        table.append("\n");

        // Data Rows
        for (Map<String, String> row : formattedRows) {
            table.append("|");
            for (String col : columnNames) {
                int width = columnWidths.get(col);
                table.append(String.format(" %-" + width + "s |", row.get(col)));
            }
            table.append("\n");
        }

        return table.toString();
    }

    /**
     * Write the content of a result set as a SQL script (CREATE TABLE and INSERT statements) into a writer.
     *
     * @param resultSetConnection Connection linked with the result set
     * @param resultSet           Result set (caller is responsible to close it)
     * @param tableName           Name of the table to create in the generated script
     * @param writer              Writer receiving the SQL script
     * @param targetDbType        Database type of the target database (H2 or PostGIS)
     * @param createTable         Insert create table statements (with drop table if exists before) and add the primary key of the source table if applicable
     * @throws SQLException if an error occurs while reading the result set
     * @throws IOException  if an error occurs while writing the script
     */
    public static void writeResultSetAsSql(Connection resultSetConnection, ResultSet resultSet, String tableName,
                                           Writer writer, DBTypes targetDbType, boolean createTable) throws SQLException, IOException {
        List<PostGISUtilities.ColumnInfo> columnInfos = PostGISUtilities.getColumnInfos(resultSetConnection, resultSet, targetDbType);
        if(createTable) {
            writer.write("DROP TABLE IF EXISTS "+tableName+";\n");
            writer.write(PostGISUtilities.createTableSql(tableName, columnInfos));
            writer.write(";\n");
        }
        while (resultSet.next()) {
            StringBuilder insertSql = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (");
            for (int i = 1; i <= columnInfos.size(); i++) {
                if (i > 1) {
                    insertSql.append(", ");
                }
                insertSql.append(formatSqlValue(resultSet.getObject(i), targetDbType));
            }
            insertSql.append(");\n");
            writer.write(insertSql.toString());
        }
        // Add primary key if applicable
        if (createTable) {
            List<TableLocation> fieldTableLocations = columnInfos.stream().map(PostGISUtilities.ColumnInfo::tableLocation).toList();
            if (!fieldTableLocations.isEmpty() && !fieldTableLocations.getFirst().getTable().isEmpty() &&
                    fieldTableLocations.stream().allMatch(t -> t.equals(fieldTableLocations.getFirst()))) {
                Tuple<String, Integer> pkInfo = JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(resultSetConnection, fieldTableLocations.getFirst());
                if (pkInfo != null && columnInfos.stream().anyMatch(c -> c.name().equalsIgnoreCase(pkInfo.first()))) {
                    writer.write("ALTER TABLE " + tableName + " ADD PRIMARY KEY (" + pkInfo.first() + ");\n");
                }
            }
        }
        writer.flush();
    }

    /**
     * Format a result set value as a SQL literal for the given target database type.
     *
     * @param value        Result set value
     * @param targetDbType Database type of the target database
     * @return The value as a SQL literal
     */
    private static String formatSqlValue(Object value, DBTypes targetDbType) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean bool) {
            return bool ? "TRUE" : "FALSE";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            return formatByteArrayLiteral(bytes, targetDbType);
        }
        if (value instanceof Geometry geometry) {
            return formatGeometryLiteral(geometry, targetDbType);
        }
        if (value instanceof java.sql.Array array) {
            try {
                return formatArrayLiteral((Object[]) array.getArray(), targetDbType);
            } catch (SQLException e) {
                // fall through to string literal
            }
        } else if (value.getClass().isArray()) {
            return formatArrayLiteral((Object[]) value, targetDbType);
        }
        return "'" + escapeSqlLiteral(value.toString()) + "'";
    }

    /**
     * Format an array as a SQL array constructor.
     *
     * @param array        Array elements
     * @param targetDbType Database type of the target database
     * @return The array as a SQL ARRAY[...] constructor
     */
    private static String formatArrayLiteral(Object[] array, DBTypes targetDbType) {
        StringBuilder arraySql = new StringBuilder("ARRAY[");
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                arraySql.append(", ");
            }
            arraySql.append(formatSqlValue(array[i], targetDbType));
        }
        return arraySql.append("]").toString();
    }

    /**
     * Format a geometry as an ST_GeomFromText expression. PostGIS receives the EWKT string while H2
     * receives the WKT string with its SRID as a second argument.
     *
     * @param geometry     Geometry value
     * @param targetDbType Database type of the target database
     * @return The geometry as an ST_GeomFromText expression
     */
    private static String formatGeometryLiteral(Geometry geometry, DBTypes targetDbType) {
        String ewkt = EWKTUtils.ewkb2ewkt(JTSUtils.geometry2ewkb(geometry));
        return "'"+escapeSqlLiteral(ewkt)+"'::geometry";
    }

    /**
     * Format a byte array as a hex literal.
     *
     * @param bytes        Byte array value
     * @param targetDbType Database type of the target database
     * @return The byte array as a hex literal
     */
    private static String formatByteArrayLiteral(byte[] bytes, DBTypes targetDbType) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(String.format(Locale.ROOT, "%02x", b & 0xff));
        }
        if (targetDbType == DBTypes.POSTGIS || targetDbType == DBTypes.POSTGRESQL) {
            return "'\\x" + hex + "'";
        }
        return "X'" + hex + "'";
    }

    /**
     * Escape a string value to be used inside a SQL string literal.
     *
     * @param value String value
     * @return The escaped string value
     */
    private static String escapeSqlLiteral(String value) {
        return value.replace("'", "''");
    }
}
