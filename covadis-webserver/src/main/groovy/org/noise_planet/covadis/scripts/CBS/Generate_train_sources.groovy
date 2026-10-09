/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.scripts.CBS

import org.h2gis.api.ProgressVisitor

import java.sql.Connection


title = 'Create trains sounds sources table in PostGIS database'
description = 'Create trains sounds sources table in PostGIS database'

inputs = [
        projectionName: [
                description: "Projection name",
                title: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {

}