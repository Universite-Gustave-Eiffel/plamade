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

package org.noise_planet.noisemodelling.wps.plamade

import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper

import org.noise_planet.noisemodelling.emission.*
import org.noise_planet.noisemodelling.pathfinder.*
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.jdbc.*
import org.noise_planet.noisemodelling.pathfinder.utils.JVMMemoryMetric
import org.noise_planet.noisemodelling.pathfinder.utils.ProfilerThread
import org.noise_planet.noisemodelling.pathfinder.utils.ProgressMetric
import org.noise_planet.noisemodelling.pathfinder.utils.ReceiverStatsMetric

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

def List<ArrayList<PointNoiseMap.CellIndex>> splitCells(Map<PointNoiseMap.CellIndex, Integer> cells, int numberOfNodes) {

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    Integer numberOfReceiver = cells.values().sum() as Integer

    int minReceiverByNodes = numberOfReceiver / numberOfNodes

    int currentNodeIndex = 0
    int currentNodeReceiversCount = 0
    ArrayList<PointNoiseMap.CellIndex> currentNodeCells = new ArrayList<>()
    List<ArrayList<PointNoiseMap.CellIndex>> resultValues = new ArrayList<>()
    resultValues.add(currentNodeCells)
    for(Map.Entry<PointNoiseMap.CellIndex, Integer> entry : cells) {
        currentNodeReceiversCount += entry.value
        currentNodeCells.add(entry.key)
        if(currentNodeReceiversCount > minReceiverByNodes) {
            currentNodeIndex += 1
            currentNodeReceiversCount = 0
            currentNodeCells = new ArrayList<>()
            resultValues.add(currentNodeCells)
        }
        int latIndex = entry.key.latitudeIndex
        int longIndex = entry.key.longitudeIndex
        logger.info(String.format(Locale.ROOT, "Node %d lat:%d long:%d",
                currentNodeIndex, latIndex, longIndex))
    }
    if(!resultValues.isEmpty() && resultValues.last().isEmpty()) {
        // remove last entry if empty
        resultValues.pop()
    }
    return resultValues
}

// main function of the script
def exec(Connection connection, Map input) {
    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : cluster config')
    logger.info("inputs {}", input) // log inputs of the run

    // -------------------
    // Get every inputs
    // -------------------

    String sources_table_name = "LW_ROADS"


    // Pointing the 'receivers' table
    String receivers_table_name = "receivers"
    // do it case-insensitive
    receivers_table_name = receivers_table_name.toUpperCase()
    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)
    List<String> geomFieldsRcv = SFSUtilities.getGeometryFields(connection, receiverTableIdentifier)
    if (geomFieldsRcv.isEmpty()) {
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier))
    }
    // Check if srid are in metric projection and are all the same.
    int sridReceivers = SFSUtilities.getSRID(connection, TableLocation.parse(receivers_table_name))
    if (sridReceivers == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+receivers_table_name+".")
    if (sridReceivers == 0) throw new IllegalArgumentException("Error : The table "+receivers_table_name+" does not have an associated SRID.")


    // Get the primary key field of the receiver table
    int pkIndexRecv = JDBCUtilities.getIntegerPrimaryKey(connection, receivers_table_name)
    if (pkIndexRecv < 1) {
        throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", receiverTableIdentifier))
    }

    // Pointing the 'buildings' table
    String building_table_name = "buildings_screens"
    // do it case-insensitive
    building_table_name = building_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridBuildings = SFSUtilities.getSRID(connection, TableLocation.parse(building_table_name))
    if (sridBuildings == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+building_table_name+".")
    if (sridBuildings == 0) throw new IllegalArgumentException("Error : The table "+building_table_name+" does not have an associated SRID.")
    if (sridReceivers != sridBuildings) throw new IllegalArgumentException("Error : The SRID of table "+building_table_name+" and "+receivers_table_name+" are not the same.")

    // Pointing the 'dem' table
    String dem_table_name = "dem"
    // do it case-insensitive
    dem_table_name = dem_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridDEM = SFSUtilities.getSRID(connection, TableLocation.parse(dem_table_name))
    if (sridDEM == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+dem_table_name+".")
    if (sridDEM == 0) throw new IllegalArgumentException("Error : The table "+dem_table_name+" does not have an associated SRID.")

    // Pointing the 'landcover' table
    String ground_table_name = "landcover"
    // do it case-insensitive
    ground_table_name = ground_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridGROUND = SFSUtilities.getSRID(connection, TableLocation.parse(ground_table_name))
    if (sridGROUND == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+ground_table_name+".")
    if (sridGROUND == 0) throw new IllegalArgumentException("Error : The table "+ground_table_name+" does not have an associated SRID.")

    // -----------------------------------------------------------------------------
    // Define and set the parameters coming from the global configuration table (CONF)
    // The parameters are selected using the confId input variable

    def row_conf = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", input.confId)
    int reflexion_order = row_conf.confreflorder.toInteger()
    int max_src_dist = row_conf.confmaxsrcdist.toInteger()
    int max_ref_dist = row_conf.confmaxrefldist.toInteger()
    int n_thread = row_conf.confthreadnumber.toInteger()
    // overwrite with the system number of thread - 1
    Runtime runtime = Runtime.getRuntime();
    n_thread = Math.max(1, runtime.availableProcessors() - 1)
    boolean compute_vertical_diffraction = row_conf.confdiffvertical
    boolean compute_horizontal_diffraction = row_conf.confdiffhorizontal
    boolean confSkipLday = row_conf.confskiplday
    boolean confSkipLevening = row_conf.confskiplevening
    boolean confSkipLnight = row_conf.confskiplnight
    boolean confSkipLden = row_conf.confskiplden
    boolean confExportSourceId = row_conf.confexportsourceid
    double wall_alpha = row_conf.wall_alpha.toDouble()

    logger.info(String.format("PARAM : You have chosen the configuration number %d ", input.confId));
    logger.info(String.format("PARAM : Reflexion order equal to %d ", reflexion_order));
    logger.info(String.format("PARAM : Maximum source distance equal to %d ", max_src_dist));
    logger.info(String.format("PARAM : Maximum reflexion distance equal to %d ", max_ref_dist));
    logger.info(String.format("PARAM : Number of thread used %d ", n_thread));
    logger.info(String.format("PARAM : The compute_vertical_diffraction parameter is %s ", compute_vertical_diffraction));
    logger.info(String.format("PARAM : The compute_horizontal_diffraction parameter is %s ", compute_horizontal_diffraction));
    logger.info(String.format("PARAM : The confSkipLday parameter is %s ", confSkipLday));
    logger.info(String.format("PARAM : The confSkipLevening parameter is %s ", confSkipLevening));
    logger.info(String.format("PARAM : The confSkipLnight parameter is %s ", confSkipLnight));
    logger.info(String.format("PARAM : The confSkipLden parameter is %s ", confSkipLden));
    logger.info(String.format("PARAM : The confExportSourceId parameter is %s ", confExportSourceId));
    logger.info(String.format("PARAM : The wall_alpha is equal to %s ", wall_alpha));

    // -----------------------------------------------------------------------------
    // Define and set the parameters coming from the ZONE table

    def row_zone = sql.firstRow("SELECT * FROM ZONE")

    double confHumidity = row_zone.hygro_d.toDouble()
    double confTemperature = row_zone.temp_d.toDouble()
    String confFavorableOccurrences = row_zone.pfav_06_18

    logger.info(String.format("PARAM : The relative humidity is set to %s ", confHumidity));
    logger.info(String.format("PARAM : The temperature is set to %s ", confTemperature));
    logger.info(String.format("PARAM : The pfav values are %s ", confFavorableOccurrences));

    // -------------------------
    // Initialize some variables
    // -------------------------

    // Set of already processed receivers
    Set<Long> receivers = new HashSet<>()

    // --------------------------------------------
    // Initialize NoiseModelling propagation part
    // --------------------------------------------

    PointNoiseMap pointNoiseMap = new PointNoiseMap(building_table_name, sources_table_name, receivers_table_name)

    LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN)

    ldenConfig_propa.setComputeLDay(!confSkipLday)
    ldenConfig_propa.setComputeLEvening(!confSkipLevening)
    ldenConfig_propa.setComputeLNight(!confSkipLnight)
    ldenConfig_propa.setComputeLDEN(!confSkipLden)
    ldenConfig_propa.setMergeSources(!confExportSourceId)
    ldenConfig_propa.setlDayTable("LDAY_ROADS")
    ldenConfig_propa.setlEveningTable("LEVENING_ROADS")
    ldenConfig_propa.setlNightTable("LNIGHT_ROADS")
    ldenConfig_propa.setlDenTable("LDEN_ROADS")
    ldenConfig_propa.setComputeLAEQOnly(true)

    LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa)
    // Add train directivity
    // TODO add optional discrete directivity table name
    ldenProcessing.insertTrainDirectivity()
    pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
    pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
    pointNoiseMap.setSoundReflectionOrder(reflexion_order)

    // Set environmental parameters
    PropagationProcessPathData environmentalData = new PropagationProcessPathData(false)

    environmentalData.setHumidity(confHumidity)
    environmentalData.setTemperature(confTemperature)

    StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ',')
    double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length]
    for (int i = 0; i < favOccurrences.length; i++) {
        favOccurrences[i] = Math.max(0, Math.min(1, Double.valueOf(tk.nextToken().trim())))
    }
    environmentalData.setWindRose(favOccurrences)

    pointNoiseMap.setPropagationProcessPathData(environmentalData)

    // Building height field name
    pointNoiseMap.setHeightField("HEIGHT")
    // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
    if (ground_table_name != "") {
        pointNoiseMap.setSoilTableName(ground_table_name)
    }
    // Point cloud height above sea level POINT(X Y Z)
    if (dem_table_name != "") {
        pointNoiseMap.setDemTable(dem_table_name)
    }

    pointNoiseMap.setMaximumPropagationDistance(max_src_dist)
    pointNoiseMap.setMaximumReflectionDistance(max_ref_dist)
    pointNoiseMap.setWallAbsorption(wall_alpha)
    pointNoiseMap.setThreadCount(n_thread)


    // --------------------------------------------
    // Initialize NoiseModelling emission part
    // --------------------------------------------

    pointNoiseMap.setComputeRaysOutFactory(ldenProcessing)
    pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing)


    // Do not propagate for low emission or far away sources
    // Maximum error in dB
    pointNoiseMap.setMaximumError(0.2d)
    //pointNoiseMap.setNoiseFloor(-50d);
    // Init Map
    pointNoiseMap.initialize(connection, new EmptyProgressVisitor())

    // --------------------------------------------
    // Run Calculations
    // --------------------------------------------

    // --------------------------------------------
    // Initialize NoiseModelling emission part
    // --------------------------------------------
    pointNoiseMap.setComputeRaysOutFactory(ldenProcessing)
    pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing)


    // Init Map
    pointNoiseMap.initialize(connection, new EmptyProgressVisitor())

    pointNoiseMap.setGridDim(25)


    // --------------------------------------------
    // Run Calculations
    // --------------------------------------------

    logger.info("Start calculation... ")

    Integer numberOfNodes = 8

    if("numberOfNodes" in input) {
        numberOfNodes = input["numberOfNodes"] as Integer
    }


    ProgressVisitor progressLogger

    if("progressVisitor" in input) {
        progressLogger = input["progressVisitor"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1);
    }


    Map<PointNoiseMap.CellIndex, Integer> cells = pointNoiseMap.searchPopulatedCells(connection)
    List<ArrayList<PointNoiseMap.CellIndex>> resultValues = splitCells(cells, numberOfNodes)

    // print to WPS Builder
    return resultValues

}
