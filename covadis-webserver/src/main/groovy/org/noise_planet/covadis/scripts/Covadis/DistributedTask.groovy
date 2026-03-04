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
package org.noise_planet.covadis.scripts.Covadis

import org.h2gis.api.ProgressVisitor
import java.sql.Connection

title = 'Distributed Noise_level_from_source'
description = 'Using the task identifier, restrict the receiver range to compute the noise levels'

inputs = [
        taskId            : [
                name       : 'Task identifier',
                title      : 'Task identifier',
                description: 'job array index value',
                type       : Integer.class
        ],
        minTaskId            : [
                name       : 'Min Task identifier',
                title      : 'Min Task identifier',
                description: 'job array min value',
                type       : Integer.class
        ],
        maxTaskId            : [
                name       : 'Max Task identifier',
                title      : 'Max Task identifier',
                description: 'job array max value',
                type       : Integer.class
        ],

]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'Result output string',
                type       : String.class
        ]
]

def exec(Connection connection, Map input, ProgressVisitor progress) {

    /**
     *
     * SLURM_ARRAY_TASK_ID will be set to the job array index value.
     * SLURM_ARRAY_TASK_COUNT will be set to the number of tasks in the job array.
     * SLURM_ARRAY_TASK_MAX will be set to the highest job array index value.
     * SLURM_ARRAY_TASK_MIN will be set to the lowest job array index value.
     */
}