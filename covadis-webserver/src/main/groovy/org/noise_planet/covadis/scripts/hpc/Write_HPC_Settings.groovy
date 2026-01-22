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

title = 'Delaunay Grid'
description = '&#10145;&#65039; Computes a <a href="https://en.wikipedia.org/wiki/Delaunay_triangulation" target="_blank">Delaunay</a> grid of receivers.</br>' +
        '<hr>' +
        'The grid will be based on: <ul>' +
        '<li> the BUILDINGS table extent (option by default)</li>' +
        '<li> <b>OR</b> a single Geometry "fence" (Extent filter).</li></ul>' +
        '&#x2705; Two tables are returned:<ul>' +
        '<li> <b>RECEIVERS</b></li>' +
        '<li> <b>TRIANGLES</b></li>' +
        '<img src="/wps_images/delaunay_grid_output.png" alt="Delaunay grid output" width="95%" align="center">'

inputs = [
        tableBuilding      : [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: 'Name of the Buildings table. </br><br>' +
                        'The table must contain: <ul>' +
                        '<li> <b> THE_GEOM </b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON)</li></ul>',
                type       : String.class
        ],
        fence              : [
                name       : 'Extent filter',
                title      : 'Extent filter',
                description: 'Create receivers only in the provided polygon (fence)',
                min        : 0, max: 1,
                type       : Geometry.class
        ],
        sourcesTableName   : [
                name       : 'Sources table name',
                title      : 'Sources table name',
                description: 'Name of the Road table.</br><br>' +
                        'Receivers will not be created on the specified road width',
                type       : String.class
        ],
        maxCellDist        : [
                name       : 'Maximum cell size',
                title      : 'Maximum cell size',
                description: 'Maximum distance used to split the domain into sub-domains (in meters) (FLOAT).</br><br>' +
                        'In a logic of optimization of processing times, it allows to limit the number of objects (buildings, roads, …) stored in memory during the Delaunay triangulation.</br></br>' +
                        '&#128736; Default value: <b>600 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        roadWidth          : [
                name       : 'Road width',
                title      : 'Road width',
                description: 'Set Road Width (in meters) (FLOAT).</br> </br>' +
                        'No receivers closer than road width distance will be created.</br> </br>' +
                        '&#128736; Default value: <b>2 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        maxArea            : [
                name       : 'Maximum Area',
                title      : 'Maximum Area',
                description: 'Set Maximum Area (in m2) (FLOAT).</br> </br>' +
                        'No triangles larger than provided area will be created.</br>' +
                        'Smaller area will create more receivers.</br> </br> ' +
                        '&#128736; Default value: <b>2500 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        height             : [
                name       : 'Height',
                title      : 'Height',
                description: 'Receiver height relative to the ground (in meters) (FLOAT).</br> </br>' +
                        '&#128736; Default value: <b>4 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        outputTableName    : [
                name       : 'outputTableName',
                title      : 'Name of output table',
                description: 'Name of the output table.</br> </br>' +
                        'Do not write the name of a table that contains a space.</br> </br>' +
                        '&#128736; Default value: <b>RECEIVERS </b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        isoSurfaceInBuildings: [
                name        : 'Create IsoSurfaces over buildings',
                title       : 'Create IsoSurfaces over buildings',
                description : 'If enabled, isosurfaces will be visible at the location of buildings </br></br>' +
                        '&#128736; Default value: <b>false </b>',
                min         : 0, max: 1,
                type        : Boolean.class
        ],
        fenceNegativeBuffer             : [
                name       : 'Negative buffer',
                title      : 'Negative buffer',
                description: 'Reduce the fence(parameter, or sound sources and buildings extent)' +
                        ' used to generate receivers positions. You should set here the maximum propagation distance (in meters) (FLOAT).</br> </br>' +
                        '&#128736; Default value: <b>0 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ]
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

    // print to WPS Builder
    return ["result" : ""]

}

