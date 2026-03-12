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
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.covadis.scripts.Import_and_Export.Export_Table

import java.sql.Connection
import java.sql.Statement

title = 'Main remote script on Slurm Cluster'
description = 'Main remote script on Slurm Cluster to run NoiseModelling on multiple nodes'

inputs = [taskId        : [name       : 'Task identifier',
                           title      : 'Task identifier',
                           description: 'job array index value.Read it from SLURM_ARRAY_TASK_ID.',
                           type       : Integer.class],
          minTaskId     : [name       : 'Min Task identifier',
                           title      : 'Min Task identifier',
                           description: 'job array min value. Read it from SLURM_ARRAY_TASK_MIN.',
                           type       : Integer.class],
          maxTaskId     : [name       : 'Max Task identifier',
                           title      : 'Max Task identifier',
                           description: 'job array max value. Read it from SLURM_ARRAY_TASK_MAX.',
                           type       : Integer.class],
          encodedNoiseLevelFromSourceInputs: [name       : 'Encoded inputs',
                           title      : 'Encoded inputs',
                           description: 'Base64 encoded inputs of the Noise_level_from_source.groovy script',
                           type       : String.class],
          outputFolder: [name       : 'Export data folder',
                                              title      : 'Export data folder',
                                              description: 'Location to save the RECEIVERS_LEVEL table',
                                              type       : String.class],

]

outputs = [result: [name       : 'Result output string',
                    title      : 'Result output string',
                    description: 'Result output string',
                    type       : String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {

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
    new org.noise_planet.covadis.scripts.NoiseModelling.Noise_level_from_source().exec(connection, decodedInputs , progress)

    // Export data
    new Export_Table().exec(connection, [tableToExport: "RECEIVERS_LEVEL", exportPath: new File(outputFolder, "RECEIVERS_LEVEL_${taskId}.geojson")], progress)

    return ["result" : "RECEIVERS_LEVEL"]
}

def filterReceivers(Connection connection, int minTaskId, int maxTaskId, int taskId, String receivers_table_name) {

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
    sql.execute("""DELETE FROM $receivers_table_name WHERE ${pkNameAndIndex.first()} < $firstReceiverPrimaryKey OR ${pkNameAndIndex.first()} > $lastReceiverPrimaryKey""".toString())

}
