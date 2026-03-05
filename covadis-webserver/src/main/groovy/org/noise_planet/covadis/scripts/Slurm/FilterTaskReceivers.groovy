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

import java.sql.Connection
import java.sql.Statement

title = 'Keep only receivers related to the current task identifier'
description = 'Using the task identifier, restrict to a computed receiver range'

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
          tableReceivers: [name       : 'Receivers table name',
                           title      : 'Receivers table name',
                           description: 'Name of the Receivers table </br> </br>' + 'The table must contain: </br> <ul>' + '<li> <b> PK </b> : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY) </li> ' + '<li> <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT) </li> </ul>' + '&#128161; This table can be generated from the WPS Blocks in the "Receivers" folder',
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
     * SLURM_ARRAY_TASK_MIN will be set to the lowest job array index value.*/
    String receivers_table_name = statement.enquoteIdentifier(input['tableReceivers'] as String, false)

    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)

    //Get the primary key field of the receiver table
    org.h2gis.utilities.Tuple<String, Integer> pkNameAndIndex = JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(connection, TableLocation.parse(receivers_table_name))

    if (pkNameAndIndex == null) {
        throw new IllegalArgumentException("Receivers table $receiverTableIdentifier does not contain a primary key")
    }

    def taskId = input['taskId'] as Integer
    def minTaskId = input['minTaskId'] as Integer
    def maxTaskId = input['maxTaskId'] as Integer

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
    sql.execute("DELETE FROM $receivers_table_name WHERE ${pkNameAndIndex.first()} < $firstReceiverPrimaryKey OR ${pkNameAndIndex.first()} > $lastReceiverPrimaryKey".toString())
    return ["result" : receivers_table_name]
}