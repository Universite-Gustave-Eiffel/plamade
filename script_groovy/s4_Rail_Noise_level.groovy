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

  /* TODO
    - Check spatial index and srids
    - Add Metadatas
    - remove unnecessary lines (il y en a beaucoup)
    - Check CONF, add some, sensibility analysis
    - Fond good compromise for NoiseFLoor and Maximum error (lignes 413)
 */


package org.noise_planet.noisemodelling.wps.plamade

import groovy.sql.Sql
import groovy.time.TimeCategory
import org.h2gis.functions.spatial.edit.ST_AddZ
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.noisemodelling.pathfinder.utils.JVMMemoryMetric
import org.noise_planet.noisemodelling.pathfinder.utils.ProfilerThread
import org.noise_planet.noisemodelling.pathfinder.utils.ProgressMetric
import org.noise_planet.noisemodelling.pathfinder.utils.ReceiverStatsMetric

import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor

import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.SpatialResultSet
import org.h2gis.utilities.wrapper.ConnectionWrapper

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.LineString

import org.noise_planet.noisemodelling.emission.*
import org.noise_planet.noisemodelling.pathfinder.*
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.jdbc.*

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException


title = 'Compute LDay, Levening, LNight and Lden from rail traffic'
description = 'Compute Lday noise map from Day Evening Night rail traffic flow rate and speed estimates (specific format, see input details).' +
        '</br> Tables must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed.' +
        '</br> </br> <b> The output tables are called : LDAY_RAILWAY LEVENING_RAILWAY LNIGHT_RAILWAY LDEN_RAILWAY </b> ' +
        'and contain : </br>' +
        '- <b> IDRECEIVER  </b> : an identifier (INTEGER, PRIMARY KEY). </br>' +
        '- <b> THE_GEOM </b> : the 3D geometry of the receivers (POINT).</br> ' +
        '- <b> Hz63, Hz125, Hz250, Hz500, Hz1000,Hz2000, Hz4000, Hz8000 </b> : 8 columns giving the day emission sound level for each octave band (FLOAT).'

inputs = [
        confId: [
                name       : 'Global configuration Identifier',
                title      : 'Global configuration Identifier',
                description: 'Id of the global configuration used for this process',
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

// run the script
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

// main function of the script
def exec(Connection connection, input) {
    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    DBTypes dbTypes = DBUtils.getDBType(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    // Update metadata table with start time
    sql.execute(String.format("UPDATE metadata SET rail_conf =" + input.confId))
    sql.execute(String.format("UPDATE metadata SET rail_start = NOW();"))

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Noise level from Traffic')
    logger.info("inputs {}", input) // log inputs of the run

    // -------------------
    // Get every inputs
    // -------------------

    // Pointing the 'rail_sections' table
    String rail_sections = "rail_sections"
    // do it case-insensitive
    rail_sections = rail_sections.toUpperCase()

    // Pointing the 'rail_traffic' table
    String rail_traffic = "rail_traffic"
    // do it case-insensitive
    rail_traffic = rail_traffic.toUpperCase()

    // Check if srid are in metric projection.
    int sridSources = GeometryTableUtilities.getSRID(connection, TableLocation.parse(rail_sections))
    if (sridSources == 3785 || sridSources == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+rail_sections+".")
    if (sridSources == 0) throw new IllegalArgumentException("Error : The table "+rail_sections+" does not have an associated SRID.")

    //Get the geometry field of the source table
    TableLocation sourceTableIdentifier = TableLocation.parse(rail_sections)
    List<String> geomFields = SFSUtilities.getGeometryFields(connection, sourceTableIdentifier)
    if (geomFields.isEmpty()) {
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", sourceTableIdentifier))
    }

    //Get the primary key field of the source table
    int pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, rail_sections)
    if (pkIndex < 1) {
        sql.execute("ALTER TABLE " + rail_sections + " ADD PK INT AUTO_INCREMENT PRIMARY KEY;")
       // throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
    }

    //Get the primary key field of the source table
    pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, rail_traffic)
    if (pkIndex < 1) {
        sql.execute("ALTER TABLE " + rail_traffic + " ADD PK INT AUTO_INCREMENT PRIMARY KEY;")
        //throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
    }

    // Pointing the 'receivers' table
    String receivers_table_name = "receivers"    
    // do it case-insensitive
    receivers_table_name = receivers_table_name.toUpperCase()
    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)
    List<String> geomFieldsRcv = GeometryTableUtilities.getGeometryColumnNames(connection,
            receiverTableIdentifier)
    if (geomFieldsRcv.isEmpty()) {
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier))
    }
    // Check if srid are in metric projection and are all the same.
    int sridReceivers = GeometryTableUtilities.getSRID(connection, TableLocation.parse(receivers_table_name))
    if (sridReceivers == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+receivers_table_name+".")
    if (sridReceivers == 0) throw new IllegalArgumentException("Error : The table "+receivers_table_name+" does not have an associated SRID.")
    if (sridReceivers != sridSources) throw new IllegalArgumentException("Error : The SRID of table "+rail_sections+" and "+receivers_table_name+" are not the same.")


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
    int sridBuildings = GeometryTableUtilities.getSRID(connection, TableLocation.parse(building_table_name))
    if (sridBuildings == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+building_table_name+".")
    if (sridBuildings == 0) throw new IllegalArgumentException("Error : The table "+building_table_name+" does not have an associated SRID.")
    if (sridReceivers != sridBuildings) throw new IllegalArgumentException("Error : The SRID of table "+building_table_name+" and "+receivers_table_name+" are not the same.")

    // Pointing the 'dem' table
    String dem_table_name = "dem"
    // do it case-insensitive
    dem_table_name = dem_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridDEM = GeometryTableUtilities.getSRID(connection, TableLocation.parse(dem_table_name))
    if (sridDEM == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+dem_table_name+".")
    if (sridDEM == 0) throw new IllegalArgumentException("Error : The table "+dem_table_name+" does not have an associated SRID.")
    if (sridDEM != sridSources) throw new IllegalArgumentException("Error : The SRID of table "+rail_sections+" and "+dem_table_name+" are not the same.")

    // Pointing the 'landcover' table
    String ground_table_name = "landcover"
    // do it case-insensitive
    ground_table_name = ground_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridGROUND = GeometryTableUtilities.getSRID(connection, TableLocation.parse(ground_table_name))
    if (sridGROUND == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+ground_table_name+".")
    if (sridGROUND == 0) throw new IllegalArgumentException("Error : The table "+ground_table_name+" does not have an associated SRID.")
    if (sridGROUND != sridSources) throw new IllegalArgumentException("Error : The SRID of table "+ground_table_name+" and "+rail_sections+" are not the same.")


    // -----------------------------------------------------------------------------
    // Define and set the parameters coming from the global configuration table (CONF)
    // The parameters are selected using the confId input variable

    def row_conf = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", input.confId)
    int reflexion_order = row_conf.confreflorder.toInteger()
    int max_src_dist = row_conf.confmaxsrcdist.toInteger()
    int max_ref_dist = row_conf.confmaxrefldist.toInteger()
    int n_thread = row_conf.confthreadnumber.toInteger()
    boolean compute_vertical_diffraction = true
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

    String sources_table_name = "LW_RAILWAY"
    PointNoiseMap pointNoiseMap = new PointNoiseMap(building_table_name, sources_table_name, receivers_table_name)

    LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN)

    ldenConfig_propa.setComputeLDay(!confSkipLday)
    ldenConfig_propa.setComputeLEvening(!confSkipLevening)
    ldenConfig_propa.setComputeLNight(!confSkipLnight)
    ldenConfig_propa.setComputeLDEN(!confSkipLden)
    ldenConfig_propa.setMergeSources(!confExportSourceId)
    ldenConfig_propa.setlDayTable("LDAY_RAILWAY")
    ldenConfig_propa.setlEveningTable("LEVENING_RAILWAY")
    ldenConfig_propa.setlNightTable("LNIGHT_RAILWAY")
    ldenConfig_propa.setlDenTable("LDEN_RAILWAY")

    LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa)
    // Add train directivity
    // TODO add optional discrete directivity table name
    ldenProcessing.insertTrainDirectivity()
    pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
    pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
    pointNoiseMap.setSoundReflectionOrder(reflexion_order)
    pointNoiseMap.setGs(1.0)

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

    // Do not propagate for low emission or far away sources
    // Maximum error in dB

    // --------------------------------------------
    // Initialize NoiseModelling emission part
    // --------------------------------------------

    pointNoiseMap.setComputeRaysOutFactory(ldenProcessing)
    pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing)


    // Do not propagate for low emission or far away sources
    // Maximum error in dB
    pointNoiseMap.setMaximumError(0.2d)
    // NoiseFloor ?
    //pointNoiseMap.setNoiseFloor(40d);

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
    logger.info("Taille de cellulle : " + pointNoiseMap.getCellWidth().toString())

    // --------------------------------------------
    // Run Calculations
    // --------------------------------------------

    // Init ProgressLogger (loading bar)
    RootProgressVisitor progressLogger = new RootProgressVisitor(1, true, 1)


    logger.info("Start calculation... ")
    ProfilerThread profilerThread = new ProfilerThread(new File("profile.csv"));
    profilerThread.addMetric(ldenProcessing);
    profilerThread.addMetric(new ProgressMetric(progressLogger))
    profilerThread.addMetric(new JVMMemoryMetric())
    profilerThread.addMetric(new ReceiverStatsMetric())
    pointNoiseMap.setProfilerThread(profilerThread);

    try {
        ldenProcessing.start()
        new Thread(profilerThread).start();
        // Iterate over computation areas
        int k = 0
        Map cells = pointNoiseMap.searchPopulatedCells(connection)
        ProgressVisitor progressVisitor = progressLogger.subProcess(cells.size())
        new TreeSet<>(cells.keySet()).each { cellIndex ->
            // Run ray propagation
            logger.info(String.format("Compute... %.3f %% (%d receivers in this cell)", 100 * k++ / cells.size(), cells.get(cellIndex)))
            IComputeRaysOut ro = pointNoiseMap.evaluateCell(connection, cellIndex.getLatitudeIndex(), cellIndex.getLongitudeIndex(), progressVisitor, receivers)
            if (ro instanceof LDENComputeRaysOut) {
                LDENPropagationProcessData ldenPropagationProcessData = (LDENPropagationProcessData) ro.inputData;
                logger.info(String.format("This computation area contains %d receivers %d sound sources and %d buildings",
                        ldenPropagationProcessData.receivers.size(), ldenPropagationProcessData.sourceGeometries.size(),
                        ldenPropagationProcessData.freeFieldFinder.getBuildingCount()));
            }
        }
    } catch(IllegalArgumentException | IllegalStateException ex) {
        System.err.println(ex);
        throw ex;
    } finally {
        profilerThread.stop();
        ldenProcessing.stop()
    }
    
    // Associate Geometry column to the table LDEN
    StringBuilder createdTables = new StringBuilder()

    if (ldenConfig_propa.computeLDay) {
        createdTables.append(" LDAY_RAILWAY")
    }
    if (ldenConfig_propa.computeLEvening) {
        createdTables.append(" LEVENING_RAILWAY")
    }
    if (ldenConfig_propa.computeLNight) {
        createdTables.append(" LNIGHT_RAILWAY")
    }
    if (ldenConfig_propa.computeLDEN) {
        createdTables.append(" LDEN_RAILWAY")
    }

    sql.execute(String.format("UPDATE metadata SET rail_end = NOW();"))

    resultString = "Calculation Done ! " + createdTables.toString() + " table(s) have been created."

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : LDAY from Traffic')

    // print to WPS Builder
    return resultString

}
