/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 **/
package org.noise_planet.covadis.scripts.Slurm

import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.noisemodelling.webserver.script.ExecutionPlan
import org.noise_planet.noisemodelling.webserver.script.Job
import org.noise_planet.noisemodelling.webserver.script.ScriptMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

title = 'Main remote script on Slurm Cluster'
description = 'Main remote script on Slurm Cluster to run NoiseModelling on multiple nodes'

inputs = [taskId                           : [name       : 'Task identifier',
                                              title      : 'Task identifier',
                                              description: 'job array index value.Read it from SLURM_ARRAY_TASK_ID.',
                                              type       : Integer.class],
          minTaskId                        : [name       : 'Min Task identifier',
                                              title      : 'Min Task identifier',
                                              description: 'job array min value. Read it from SLURM_ARRAY_TASK_MIN.',
                                              type       : Integer.class],
          maxTaskId                        : [name       : 'Max Task identifier',
                                              title      : 'Max Task identifier',
                                              description: 'job array max value. Read it from SLURM_ARRAY_TASK_MAX.',
                                              type       : Integer.class],
          encodedNoiseLevelFromSourceInputs: [name       : 'Encoded inputs',
                                              title      : 'Encoded inputs',
                                              description: 'Base64 encoded inputs of the Noise_level_from_source.groovy script',
                                              type       : String.class],
          outputFolder                     : [name       : 'Export data folder',
                                              title      : 'Export data folder',
                                              description: 'Location to save the RECEIVERS_LEVEL table',
                                              type       : String.class],

]

outputs = [result: [name       : 'Result output string',
                    title      : 'Result output string',
                    description: 'Result output string',
                    type       : String.class]]
@CompileStatic
def exec(DataSource dataSource, Map input, ProgressVisitor progress) {

    try(Connection connection = dataSource.getConnection()) {
        Statement statement = connection.createStatement()

        /**
         * SLURM_ARRAY_TASK_ID will be set to the job array index value.
         * SLURM_ARRAY_TASK_COUNT will be set to the number of tasks in the job array.
         * SLURM_ARRAY_TASK_MAX will be set to the highest job array index value.
         * SLURM_ARRAY_TASK_MIN will be set to the lowest job array index value.
         **/
        String outputFolder = input['outputFolder'] as String

        // 1. Decode Base64 to bytes
        byte[] decodedBytes = (input['encodedNoiseLevelFromSourceInputs'] as String).decodeBase64()

        // 2. Deserialize bytes back to Map
        def bais = new ByteArrayInputStream(decodedBytes)
        def decodedInputs = (Map) (new ObjectInputStream(bais).withCloseable { it.readObject() })

        String receivers_table_name = statement.enquoteIdentifier(decodedInputs['tableReceivers'] as String, false)

        def taskId = input['taskId'] as Integer
        def minTaskId = input['minTaskId'] as Integer
        def maxTaskId = input['maxTaskId'] as Integer

        // Remove receivers not to be processed by this task
        filterReceivers(connection, minTaskId, maxTaskId, taskId, receivers_table_name)

        // Run NoiseLevelFromSource with the limited set of receivers
        ScriptMetadata scriptMetadata = new ScriptMetadata("NoiseModelling", new File(outputFolder, "Noise_level_from_source.groovy").toURI(), new File(outputFolder).toURI());
        ExecutionPlan executionPlan = new ExecutionPlan(decodedInputs, scriptMetadata);
        Object result = Job.runScript(executionPlan, progress, dataSource);
        // here if using original script
        //new org.noise_planet.covadis.scripts.NoiseModelling.Noise_level_from_source().exec(connection, decodedInputs , progress)
        // remove alias/ index / primary key
        dropH2GISAliases(connection)
        dropTableIndex(connection, "PUBLIC", "RECEIVERS_LEVEL")

        // Export data
        Sql sql = new Sql(connection)
        sql.execute("SCRIPT NOPASSWORDS NOSETTINGS TO '$outputFolder/RECEIVERS_LEVEL_${taskId}.sql.gz' COMPRESSION GZIP TABLE RECEIVERS_LEVEL".toString())
    }
    return ["result" : "RECEIVERS_LEVEL"]
}

static void dropTableIndex(Connection conn, String schema, String tableName) throws SQLException {
    String findIndexSql =
            "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES " +
                    "WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? " +
                    "AND INDEX_TYPE_NAME != 'PRIMARY KEY' LIMIT 1";

    try (PreparedStatement ps = conn.prepareStatement(findIndexSql)) {
        ps.setString(1, tableName.toUpperCase());
        ps.setString(2, schema.toUpperCase());

        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                try (Statement stmt = conn.createStatement()) {
                    // Always wrap names in double quotes for safety
                    stmt.execute("DROP INDEX \"" + schema + "\".\"" + indexName + "\"");
                    System.out.println("Dropped index: " + indexName);
                }
            }
        }
    }
}
public static void dropH2GISAliases(Connection conn) throws SQLException {
    String query = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES " +
            "WHERE ROUTINE_BODY = 'EXTERNAL' AND EXTERNAL_NAME LIKE 'org.h2gis.%'";

    List<String> dropCommands = new ArrayList<>();

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
            String schema = rs.getString("ROUTINE_SCHEMA");
            String name = rs.getString("ROUTINE_NAME");
            String routineType = rs.getString("ROUTINE_TYPE");
            // Use DROP ALIAS for functions linked to Java methods
            if(routineType == "FUNCTION" || routineType == "PROCEDURE") {
                dropCommands.add("DROP ALIAS IF EXISTS \"" + schema + "\".\"" + name + "\"");
            } else if(routineType == "AGGREGATE") {
                dropCommands.add("DROP AGGREGATE IF EXISTS \"" + schema + "\".\"" + name + "\"");
            }
        }
    }

    // Execute the drops
    try (Statement stmt = conn.createStatement()) {
        for (String sql : dropCommands) {
            stmt.execute(sql);
        }
    }
}

static def filterReceivers(Connection connection, int minTaskId, int maxTaskId, int taskId, String receivers_table_name) {

    DBTypes dbType = DBUtils.getDBType(connection)

    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name, dbType)

    //Get the primary key field of the receiver table
    org.h2gis.utilities.Tuple<String, Integer> pkNameAndIndex = JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(connection, receiverTableIdentifier)

    if (pkNameAndIndex == null) {
        throw new IllegalArgumentException("Receivers table $receiverTableIdentifier does not contain a primary key")
    }
    // Count the number of receivers
    String countReceiversQuery = "SELECT COUNT(*) FROM $receivers_table_name"
    Sql sql = new Sql(connection)
    int totalReceivers = sql.firstRow(countReceiversQuery)[0] as Integer
    int receiversPerTask = (int) Math.ceil((double) totalReceivers / (maxTaskId - minTaskId + 1))
    int offset = (taskId - minTaskId) * receiversPerTask
    int limit = Math.min(receiversPerTask, totalReceivers - offset)
    // Order the receivers table using the primary key
    // delete receivers not to be processed by this task
    int firstReceiverPrimaryKey = sql.firstRow("SELECT ${pkNameAndIndex.first()} FROM $receivers_table_name ORDER BY ${pkNameAndIndex.first()} LIMIT 1 OFFSET $offset".toString())[0] as Integer
    int lastReceiverPrimaryKey = sql.firstRow("SELECT ${pkNameAndIndex.first()} FROM $receivers_table_name ORDER BY ${pkNameAndIndex.first()} LIMIT 1 OFFSET ${offset + limit - 1}".toString())[0] as Integer
    Logger logger = LoggerFactory.getLogger("Main_Remote_Script")
    logger.info("Task $taskId processing receivers with primary keys between $firstReceiverPrimaryKey and $lastReceiverPrimaryKey (total: $totalReceivers, per task: $receiversPerTask)")
    sql.execute("""DELETE FROM $receivers_table_name WHERE ${pkNameAndIndex.first()} < $firstReceiverPrimaryKey OR ${pkNameAndIndex.first()} > $lastReceiverPrimaryKey""".toString())

}
