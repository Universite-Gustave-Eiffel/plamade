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

/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */


package org.noise_planet.covadis.scripts.hpc


import org.h2gis.api.ProgressVisitor
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor

import java.sql.Connection
import java.sql.Statement

title = 'Write HPC settings'
description = 'Create a table that will contain all settings to connect to a Slurm service through SSH'

inputs = [
        host      : [
                name       : 'Host',
                title      : 'Server host name or ip address',
                description: 'Ex: slurmcluster.com',
                type       : String.class
        ],
        port              : [
                name       : 'Host SSH port',
                title      : 'Host SSH port',
                description: 'Connection port to the host (default 22)',
                min        : 0, max: 1,
                type       : Integer.class
        ],
        ssl_key   : [
                name       : 'SSL Server Public Key',
                title      : 'SSL Server Public Key',
                description: '<p>Base64 Public SSL server key for host key checking</p>',
                type       : String.class
        ],
        ssk_key_type   : [
                name       : 'SSH server key type',
                title      : 'SSH server key type',
                description: '<p>SSH supported server key type ex:ssh-rsa</p>',
                type       : String.class
        ],
        user   : [
                name       : 'SSH User name',
                title      : 'SSH User name',
                description: 'Username to connect with',
                type       : String.class
        ],
        key   : [
                name       : 'SSH User Private Key',
                title      : 'SSH User Private Key',
                description: '<p>Armored SSH Private key to connect to the SSH server:</p><p># macOS</p>' +
                        '<pre>gpg --armor --export-secret-key joe@foo.bar | pbcopy</pre>' +
                        '<p># Ubuntu (assuming GNU base64)</p>' +
                        '<pre>gpg --armor --export-secret-key joe@foo.bar -w0 | xclip</pre>',
                type       : String.class
        ],
        key_password        : [
                name       : 'SSH Private Key password',
                title      : 'SSH Private Key password',
                description: 'Optional private key password',
                min        : 0, max: 1,
                type       : String.class
        ],
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]

def exec(Connection connection, Map input) {

    ProgressVisitor progressLogger

    if("_progression" in input) {
        progressLogger = input["_progression"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1)
    }

    // Create a connection statement to interact with the database in SQL
    Statement stmt = connection.createStatement()

    stmt.execute("CREATE TABLE IF NOT EXISTS SLURM_CONFIGURATION(KEY VARCHAR, VALUE VARCHAR)")
    stmt.execute("TRUNCATE TABLE SLURM_CONFIGURATION")
    input.forEach { key, value ->
        def ps = connection.prepareStatement("INSERT INTO SLURM_CONFIGURATION(KEY, VALUE) VALUES(?, ?);")
        ps.setString(1, String.valueOf(key))
        ps.setString(2, String.valueOf(value))
        ps.execute()
    }


    // print to WPS Builder
    return ["result" : "Table SLURM_CONFIGURATION updated"]

}

