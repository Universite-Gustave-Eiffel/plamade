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

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection

title = 'Buildings Grid'
description = 'Generates receivers placed 2 meters from building facades at specified height.' +
        '</br> </br> <b> The output table is called : RECEIVERS </b>'

inputs = [
        dbthreshold: [
                name       : 'Population dB threshold',
                title      : 'Population dB threshold',
                description: 'dB threshold used to filter population',
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
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info(String.format("PARAM : You have chosen to filter population exposed to more than %d ", input.dbthreshold, " db"));

    dbthreshold = input["dbthreshold"]

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)

    sql.execute(String.format("DROP TABLE IF EXISTS LDEN_GEOM_ROADS, RECEIVERS_SUM_LAEQPA, RECEIVERS_POP, ROADS_POP;"))

    sql.execute(String.format("CREATE TABLE LDEN_GEOM_ROADS AS SELECT a.the_geom, a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa, b.id_troncon, b.id_route, b.nom_route, b.pk FROM LDEN_GEOM a, roads b WHERE a.idsource=b.pk;"))

    sql.execute(String.format("CREATE TABLE RECEIVERS_SUM_LAEQPA AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, id_route, nom_route, idreceiver, 10*log10(sum(LAEQpa)) as laeqpa_sum FROM LDEN_GEOM_ROADS GROUP BY idreceiver, id_route, nom_route;"))

    sql.execute(String.format("CREATE TABLE RECEIVERS_POP AS SELECT a.id_route, a.nom_route, a.idreceiver, b.pop as pop FROM RECEIVERS_SUM_LAEQPA a, receivers b WHERE a.idreceiver=b.PK and a.laeqpa_sum>"+dbthreshold+";"))

    sql.execute(String.format("CREATE TABLE ROADS_POP AS SELECT ID_ROUTE, nom_route, ROUND(SUM(pop),1) as sum_pop FROM RECEIVERS_POP GROUP BY ID_ROUTE, nom_route ORDER BY nom_route;"))

    sql.execute(String.format("DROP TABLE LDEN_GEOM_ROADS, RECEIVERS_SUM_LAEQPA, RECEIVERS_POP;"))

    resultString = "The table ROADS_POP has been created"

    return resultString
}