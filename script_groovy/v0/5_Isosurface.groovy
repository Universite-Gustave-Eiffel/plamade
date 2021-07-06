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
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285 
 */


//package org.noise_planet.noisemodelling.wps

import geoserver.GeoServer
import geoserver.catalog.Store
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.jdbc.BezierContouring
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import groovy.sql.Sql

import org.h2gis.utilities.JDBCUtilities

title = 'Create an isosurface from a NoiseModelling result table and its associated TRIANGLES table.'

description = 'Create an isosurface from a NoiseModelling result table and its associated TRIANGLES table. The Triangle vertices table must have been created using the WPS block <b> Receivers/Delaunay_Grid </b> . ' +
        '</br> </br> <b> The output table is called :  CONTOURING_NOISE_MAP'

inputs = [
        Ready: [
                name       : 'Isosurfaces',
                title      : 'Produire des isosurfaces',
                description: 'Produire des isosurfaces pour les tables LDEN_GEOM et LNIGHT_GEOM. </br></br>Écrivez "OK"',
                type       : String.class
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

def exec(Connection connection, input) {

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // output string, the information given back to the user
    String resultString = "Le processus est terminé. En fonction des tables d'entrée, les tables CONTOURING_LDEN et CONTOURING_LNIGHT ont été créées."

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Compute Isosurfaces')
    logger.info("inputs {}", input) // log inputs of the run


    // List of input tables : inputTable
    String ldenInput = "LDEN_GEOM"
    String lnightInput = "LNIGHT_GEOM"

    // List of iso classes :  isoClasses
    // LDEN classes : 55-59, 60-64, 65-69, 70-74 et >75 dB
    def isoLevelsLDEN=[55.0d,60.0d,65.0d,70.0d,75.0d,200.0d]
    // LNIGHT classes : 50-54, 55-59, 60-64, 65-69 et >70 dB
    def isoLevelsLNIGHT=[50.0d,55.0d,60.0d,65.0d,70.0d,200.0d]

    // List of output tables : outputTable
    String ldenOutput = "CONTOURING_LDEN"
    String lnightOutput = "CONTOURING_LNIGHT"

    // Produce isocontours with following arguments
    generateIsoSurfaces(ldenInput, isoLevelsLDEN, ldenOutput, connection)
    generateIsoSurfaces(lnightInput, isoLevelsLNIGHT, lnightOutput, connection)

    // print to WPS Builder
    return resultString
}

/**
 * Generate isosurfaces for LDEN and LNIGHT tables
 * @param inputTable
 * @param isoClasses
 * @param outputTable
 * @param connection
 * @return
 */
def generateIsoSurfaces(def inputTable, def isoClasses, def outputTable, def connection) {

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    if(!JDBCUtilities.tableExists(connection, inputTable)) {
        logger.info "La table $inputTable n'est pas présente"
        return
        }
        int srid = SFSUtilities.getSRID(connection, TableLocation.parse(inputTable))

        BezierContouring bezierContouring = new BezierContouring(isoClasses, srid)

        bezierContouring.setPointTable(inputTable)
        double coefficient = 1
        bezierContouring.setSmooth(true)
        bezierContouring.setSmoothCoefficient(coefficient)

        bezierContouring.createTable(connection)

        Sql sql = new Sql(connection)
        sql.execute(String.format("DROP TABLE IF EXISTS " + outputTable))
        sql.execute(String.format("ALTER TABLE CONTOURING_NOISE_MAP RENAME TO " + outputTable))

        resultString = "Table " + outputTable + " created"

        logger.info('End : Compute Isosurfaces')
        logger.info(resultString)
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