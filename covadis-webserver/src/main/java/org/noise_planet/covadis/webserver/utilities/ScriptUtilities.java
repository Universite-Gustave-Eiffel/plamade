package org.noise_planet.covadis.webserver.utilities;

import groovy.lang.MetaMethod;
import groovy.lang.Script;
import groovy.sql.Sql;
import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.noise_planet.noisemodelling.webserver.script.ExecutionPlan;
import org.noise_planet.noisemodelling.webserver.script.ScriptMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.*;

public class ScriptUtilities {

    public static Object execScript(Script script, Connection connection, Map inputs) {
        return execScript(script, connection, inputs, new EmptyProgressVisitor());
    }

    public static Object execScript(Script script, Connection connection, Map inputs, ProgressVisitor progress) {
        Logger logger = LoggerFactory.getLogger(ScriptUtilities.class);
        inputs = ScriptUtilities.fillDefaultValues(script.getClass(), inputs);
        logger.info("Run script: {} with inputs \n{}", script.getClass().getSimpleName(),
                ((Map<String, Object>) inputs).entrySet().stream().
                        map(entry -> entry.getKey() + "=" + entry.getValue()).
                        collect(java.util.stream.Collectors.joining(", \n")));

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
        return filledInputs;
    }

    /**
     * Executes a SQL query and return the result set as a Markdown table.
     *
     * @param sql         An instance of groovy.sql.Sql
     * @param query       A String or GString
     * @param maxColWidth Maximum width of a column before truncation
     */
    public static String formatSqlQueryResult(Sql sql, Object query, int maxColWidth) {
        List<Map<String, Object>> rawRows = new ArrayList<>();
        try {
            List<?> rows = sql.rows(query.toString());
            for (Object row : rows) {
                if (row instanceof Map) {
                    rawRows.add((Map<String, Object>) row);
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger("Logging").error("Error executing SQL query: {}", query, e);
            return "";
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
}
