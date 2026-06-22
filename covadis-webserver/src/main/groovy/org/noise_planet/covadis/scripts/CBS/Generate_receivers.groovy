package org.noise_planet.covadis.scripts.CBS


import groovy.sql.Sql
import org.h2gis.api.ProgressVisitor
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.noisemodelling.scripts.Receivers.Delaunay_Grid
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

title = 'Create receivers table in PostGIS database'
description = 'Create receivers table in PostGIS database'

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
        fenceTableName: [
                name       : 'Fence table name',
                title      : 'Fence table name',
                description: 'Use the extent of a geometry table (e.g., from a shapefile) to limit receiver area',
                min        : 0, max: 1,
                type       : String.class
        ],
        tableBuilding      : [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: 'Name of the Buildings table. </br><br>' +
                        'The table must contain: <ul>' +
                        '<li> <b> THE_GEOM </b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON)</li></ul>',
                type       : String.class
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
                        'In a logic of optimization of processing times, it allows to limit the number of objects (buildings, roads, …) stored in memory during the Delaunay triangulation',
                default    : 600,
                type       : Double.class
        ],
        skipCellNoSourcesMinimalDistance        : [
                name       : 'Skip cell no sources minimal distance',
                title      : 'Skip cell no sources minimal distance',
                description: 'If provided, a sub-domain will not be computed if no sources geometries are near x meters from the sub-domain area',
                min        : 0, max: 1,
                type       : Double.class
        ],
        maxArea            : [
                name       : 'Maximum Area',
                title      : 'Maximum Area',
                description: 'Set Maximum Area (in m2) (FLOAT).</br> </br>' +
                        'No triangles larger than provided area will be created.</br>' +
                        'Smaller area will create more receivers',
                default    : 2500,
                type       : Double.class
        ],
        outputTableName    : [
                name       : 'outputTableName',
                title      : 'Name of output table',
                description: 'Name of the output table.</br> </br>' +
                        'Do not write the name of a table that contains a space',
                default    : 'RECEIVERS',
                type       : String.class
        ],
        isoSurfaceInBuildings: [
                name        : 'Create IsoSurfaces over buildings',
                title       : 'Create IsoSurfaces over buildings',
                description : 'If enabled, isosurfaces will be visible at the location of buildings',
                default    : false,
                type        : Boolean.class
        ],
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger("tutorial")

    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            input['pgUser'] as String,
            input['pgPassword'] as String,
            input['pgPort'].toString(), input['pgDatabase'] as String, input['pgHost'] as String);
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        Sql sql = new Sql(pgConnection)

        new Delaunay_Grid().exec(pgConnection, [
                fenceTableName: input['fenceTableName'] as String,
                tableBuilding: input['tableBuilding'] as String,
                sourcesTableName: input['sourcesTableName'] as String,
                maxCellDist: input['maxCellDist'] as Double,
                skipCellNoSourcesMinimalDistance: input['skipCellNoSourcesMinimalDistance'] as Double,
                maxArea: input['maxArea'] as Double,
                outputTableName: input['outputTableName'] as String,
                isoSurfaceInBuildings: input['isoSurfaceInBuildings'] as Boolean])

    } catch (SQLException e) {
        logger.error("Error connecting to PostgreSQL database: ${e.message}")
    }

    // Return results
    return [result: input['outputTableName'] as String]
}
