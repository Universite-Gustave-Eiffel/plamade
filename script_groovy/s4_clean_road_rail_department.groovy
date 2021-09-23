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
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285 
 */

package org.noise_planet.noisemodelling.wps.plamade;

import geoserver.GeoServer
import geoserver.catalog.Store
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.jdbc.BezierContouring
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import groovy.sql.Sql

import org.h2gis.utilities.JDBCUtilities

title = 'Remove roads and rail sections that are outside the studied department.'

description = 'Remove roads and rail sections that are outside the studied department.'

inputs = [
        rail_or_road: [
                name       : 'Rail or Road',
                title      : 'Rail or Road',
                description: 'Rail (1) ou Road (2)',
                type: Integer.class
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

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start cleaning roads or rail sections')
    logger.info("inputs {}", input) // log inputs of the run

    Integer railRoad = input["rail_or_road"]

    ProgressVisitor progressLogger

    if("progressVisitor" in input) {
        progressLogger = input["progressVisitor"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1);
    }
    

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)

    String codeDept = sql.firstRow("SELECT CODE_DEPT as codeDept FROM METADATA").codeDept
    
    if (railRoad==1){
        sql.execute("DELETE FROM RAIL_SECTIONS WHERE SUBSTRING(UUEID FROM 8 FOR 2) <> $codeDept;")
    }
    else{
        sql.execute("DELETE FROM ROADS WHERE SUBSTRING(UUEID FROM 8 FOR 2) <> $codeDept;")
    }

    logger.info("Le processus est terminé - Les infrastructures en dehors du département ont été supprimées")

    // print to WPS Builder
    // output string, the information given back to the user
    String resultString = "Le processus est terminé - Les infrastructures en dehors du département ont été supprimées"
    return resultString
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


