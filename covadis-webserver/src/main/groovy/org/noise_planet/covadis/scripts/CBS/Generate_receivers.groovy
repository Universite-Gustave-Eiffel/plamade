package org.noise_planet.covadis.scripts.CBS


import groovy.sql.Sql
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
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

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if(!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
        throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
    }

    def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            postgisConfig['user_name'] as String,
            postgisConfig['password'] as String,
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
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
                isoSurfaceInBuildings: input['isoSurfaceInBuildings'] as Boolean], progress)

    }

    // Return results
    return [result: input['outputTableName'] as String]
}
