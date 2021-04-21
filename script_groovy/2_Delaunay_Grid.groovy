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


package org.noise_planet.noisemodelling.wps.Receivers

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

import org.noise_planet.noisemodelling.emission.*
import org.noise_planet.noisemodelling.pathfinder.*
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.jdbc.*

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

title = 'Delaunay Grid'
description = 'Calculates a delaunay grid of receivers based on a single Geometry geom or a table tableName of Geometries with delta as offset in the Cartesian plane in meters.'

inputs = [
        confId: [
                name       : 'Global configuration Identifier',
                title      : 'Global configuration Identifier',
                description: 'Id of the global configuration used for this process',
                type: Integer.class
        ],
        fence              : [
                name       : 'Fence geometry', title: 'Extent filter',
                description: 'Create receivers only in the provided polygon',
                min        : 0, max: 1,
                type       : Geometry.class
        ],
        roadWidth          : [
                name       : 'Source Width',
                title      : 'Source Width',
                description: 'Set Road Width in meters. No receivers closer than road width distance.(FLOAT) ' +
                        '</br> </br> <b> Default value : 2 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        maxArea            : [
                name       : 'Maximum Area',
                title      : 'Maximum Area',
                description: 'Set Maximum Area in m2. No triangles larger than provided area. Smaller area will create more receivers. (FLOAT)' +
                        '</br> </br> <b> Default value : 2500 </b> ',
                min        : 0, max: 1,
                type       : Double.class
        ],
//        sourceDensification: [
//                name       : 'Source densification',
//                title      : 'Source densification',
//                description: 'Set additional receivers near sound sources (roads). This is the maximum distance between the points that compose the polygon near the source in meter. (FLOAT)' +
//                        '</br> </br> <b> Default value : 8 </b> ',
//                min        : 0, max: 1,
//                type       : Double.class
//        ],
        height             : [
                name       : 'Height',
                title      : 'Height',
                description: ' Receiver height relative to the ground in meters (FLOAT).' +
                        '</br> </br> <b> Default value : 4.1 </b>',
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


static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}


def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Delaunay grid')
    logger.info("inputs {}", input) // log inputs of the run


    String receivers_table_name = "RECEIVERS"
    receivers_table_name = receivers_table_name.toUpperCase()

    String sources_table_name = "ROADS"
    sources_table_name = sources_table_name.toUpperCase()

    String building_table_name = "BUILDINGS"
    building_table_name = building_table_name.toUpperCase()

    Double height = 4.1
    if (input['height']) {
        height = input['height'] as Double
    }

    Double roadWidth = 2.0
    if (input['roadWidth']) {
        roadWidth = input['roadWidth'] as Double
    }

    Double maxArea = 2500
    if (input.containsKey('maxArea')) {
        maxArea = input['maxArea'] as Double
    }

    //Double sourceDensification = 8.0
    //if (input.containsKey('sourceDensification')) {
    //    sourceDensification = input['sourceDensification'] as Double
    //}


    int srid = SFSUtilities.getSRID(connection, TableLocation.parse(building_table_name))

    Geometry fence = null
    WKTReader wktReader = new WKTReader()
    if (input['fence']) {
        fence = wktReader.read(input['fence'] as String)
    }

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)
    connection = new ConnectionWrapper(connection)
    RootProgressVisitor progressLogger = new RootProgressVisitor(1, true, 1)


    def row_conf = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", input.confId)
    
    Double maxPropDist = row_conf.confmaxsrcdist.toDouble()
    logger.info(String.format("PARAM : Maximum source distance equal to %s ", maxPropDist));



    // Delete previous receivers grid
    sql.execute(String.format("DROP TABLE IF EXISTS %s", receivers_table_name))
    sql.execute("DROP TABLE IF EXISTS TRIANGLES")

    // Generate receivers grid for noise map rendering
    TriangleNoiseMap noiseMap = new TriangleNoiseMap(building_table_name, sources_table_name)

    if (fence != null) {
        // Reproject fence
        int targetSrid = SFSUtilities.getSRID(connection, TableLocation.parse(building_table_name))
        if (targetSrid == 0) {
            targetSrid = SFSUtilities.getSRID(connection, TableLocation.parse(sources_table_name))
        }
        if (targetSrid != 0) {
            // Transform fence to the same coordinate system than the buildings & sources
            fence = ST_Transform.ST_Transform(connection, ST_SetSRID.setSRID(fence, 4326), targetSrid)
            noiseMap.setMainEnvelope(fence.getEnvelopeInternal())
        } else {
            System.err.println("Unable to find buildings or sources SRID, ignore fence parameters")
        }
    }


    // Avoid loading to much geometries when doing Delaunay triangulation
    noiseMap.setMaximumPropagationDistance(maxPropDist)
    // Receiver height relative to the ground
    noiseMap.setReceiverHeight(height)
    // No receivers closer than road width distance
    noiseMap.setRoadWidth(roadWidth)
    // No triangles larger than provided area
    noiseMap.setMaximumArea(maxArea)
    // Densification of receivers near sound sources
    //modifNico noiseMap.setSourceDensification(sourceDensification)

    logger.info("Delaunay initialize")
    noiseMap.initialize(connection, new EmptyProgressVisitor())
    noiseMap.setExceptionDumpFolder("data_dir/")
    AtomicInteger pk = new AtomicInteger(0)
    ProgressVisitor progressVisitorNM = progressLogger.subProcess(noiseMap.getGridDim() * noiseMap.getGridDim())

    for (int i = 0; i < noiseMap.getGridDim(); i++) {
        for (int j = 0; j < noiseMap.getGridDim(); j++) {
            logger.info("Compute cell " + (i * noiseMap.getGridDim() + j + 1) + " of " + noiseMap.getGridDim() * noiseMap.getGridDim())
            noiseMap.generateReceivers(connection, i, j, receivers_table_name, "TRIANGLES", pk)
            progressVisitorNM.endStep()
        }
    }

    //modifNico sql.execute("UPDATE " + receivers_table_name + " SET THE_GEOM = ST_SETSRID(THE_GEOM, " + srid + ")")

    logger.info("Create spatial index on "+receivers_table_name+" table")
    sql.execute("Create spatial index on " + receivers_table_name + "(the_geom);")

    int nbReceivers = sql.firstRow("SELECT COUNT(*) FROM " + receivers_table_name)[0] as Integer

    // Process Done
    resultString = "Process done. " + receivers_table_name + " (" + nbReceivers + " receivers) and TRIANGLES tables created. "

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Delaunay grid')

    // print to WPS Builder
    return resultString

}

