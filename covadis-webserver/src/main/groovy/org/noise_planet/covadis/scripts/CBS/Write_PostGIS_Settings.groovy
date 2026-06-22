/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.scripts.CBS

import org.h2gis.api.ProgressVisitor
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement

title = 'Write PostGIS settings'
description = 'Create a table named POSTGIS_CONFIGURATION that will contain all settings to connect to a remote database'

inputs = [
        pgUser: [
                description: "PostgreSQL user name",
                title: "PostgreSQL user name",
                default: 'noisemodelling',
                type: String.class
        ],
        pgPassword: [
                description: "PostgreSQL user password",
                title: "PostgreSQL user password",
                default: 'noisemodelling',
                type: String.class
        ],
        pgPort: [
                description: "PostgreSQL port",
                title: "PostgreSQL port",
                default: 5432,
                type: Integer.class
        ],
        pgDatabase: [
                description: "PostgreSQL database name",
                title: "PostgreSQL database name",
                default: 'noisemodelling_db',
                type: String.class
        ],
        pgHost: [
                description: "PostgreSQL host",
                title: "PostgreSQL host",
                default: 'localhost',
                type: String.class
        ],
]

outputs = [
        result: [
                name       : 'Configuration table name',
                title      : 'Configuration table name',
                description: 'Name of the table that contains PostGIS connection settings',
                type       : String.class
        ]
]

def exec(Connection connection, Map input) {

    // Create a connection statement to interact with the database in SQL
    Statement stmt = connection.createStatement()
    stmt.execute("DROP TABLE IF EXISTS POSTGIS_CONFIGURATION")
    stmt.execute("CREATE TABLE IF NOT EXISTS POSTGIS_CONFIGURATION(" +
            "host varchar," +
            "port integer," +
            "user_name varchar," +
            "password varchar," +
            "database_name varchar)")
    try(PreparedStatement insertSt = connection.prepareStatement("INSERT INTO POSTGIS_CONFIGURATION(" +
            "host, port, user_name, password, database_name) " +
            "VALUES(?, ?, ?, ?, ?)")) {
        insertSt.setString(1, input['pgHost'] as String)
        insertSt.setObject(2, input.getOrDefault('pgPort', 5432) as Integer)
        insertSt.setString(3, input['pgUser'] as String)
        insertSt.setString(4, input['pgPassword'] as String)
        insertSt.setString(5, input['pgDatabase'] as String)
        insertSt.executeUpdate()
    }

    // print to WPS Builder
    return ["result" : "POSTGIS_CONFIGURATION"]

}

