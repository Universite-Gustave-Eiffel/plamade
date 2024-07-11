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
 - Automatic conf because serveur down
*/


package org.noise_planet.noisemodelling.wps.plamade

import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.util.ScriptReader
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.Coordinate
import org.noise_planet.noisemodelling.jdbc.*
import org.noise_planet.noisemodelling.pathfinder.IComputeRaysOut
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.noise_planet.noisemodelling.pathfinder.utils.*
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.sql.Connection
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream

title = 'Compute LDay,Levening,LNight,Lden from road traffic'
description = 'Compute Lday noise map from Day Evening Night traffic flow rate and speed estimates (specific format, see input details).' +
        '</br> Tables must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed.' +
        '</br> </br> <b> The output tables are called : LDAY_ROADS LEVENING_ROADS LNIGHT_ROADS LDEN_ROADS </b> ' +
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
        ],
        paramWallAlpha          : [
                name       : 'wallAlpha',
                title      : 'Wall absorption coefficient',
                description: 'Wall absorption coefficient (FLOAT) </br> </br>' +
                        'This coefficient is going <br> <ul>' +
                        '<li> from 0 : fully absorbent </li>' +
                        '<li> to strictly less than 1 : fully reflective. </li> </ul>' +
                        '&#128736; Default value: <b>0.1 </b> ',
                min        : 0, max: 1,
                type       : String.class
        ],
        confReflOrder           : [
                name       : 'Order of reflexion',
                title      : 'Order of reflexion',
                description: 'Maximum number of reflections to be taken into account (INTEGER). </br> </br>' +
                        '&#x1F6A8; Adding 1 order of reflexion can significantly increase the processing time. </br> </br>' +
                        '&#128736; Default value: <b>1 </b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confMaxSrcDist          : [
                name       : 'Maximum source-receiver distance',
                title      : 'Maximum source-receiver distance',
                description: 'Maximum distance between source and receiver (in meters). </br> </br>' +
                        '&#128736; Default value: <b>150 </b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confMaxReflDist         : [
                name       : 'Maximum source-reflexion distance',
                title      : 'Maximum source-reflexion distance',
                description: 'Maximum reflection distance from the source (in meters). </br> </br>' +
                        '&#128736; Default value: <b>50 </b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confThreadNumber        : [
                name       : 'Thread number',
                title      : 'Thread number',
                description: 'Number of thread to use on the computer (INTEGER). </br> </br>' +
                        'To set this value, look at the number of cores you have. </br>' +
                        'If it is set to 0, use the maximum number of cores available.</br> </br>' +
                        '&#128736; Default value: <b>0 </b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confDiffVertical        : [
                name       : 'Diffraction on vertical edges',
                title      : 'Diffraction on vertical edges',
                description: 'Compute or not the diffraction on vertical edges. Following Directive 2015/996, enable this option for rail and industrial sources only. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1,
                type       : Boolean.class
        ],
        confDiffHorizontal      : [
                name       : 'Diffraction on horizontal edges',
                title      : 'Diffraction on horizontal edges',
                description: 'Compute or not the diffraction on horizontal edges. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1,
                type       : Boolean.class
        ],
        confSkipLday            : [
                name       : 'Skip LDAY_GEOM table',
                title      : 'Do not compute LDAY_GEOM table',
                description: 'Skip the creation of this table. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1,
                type       : Boolean.class
        ],
        confSkipLevening        :
                [name       : 'Skip LEVENING_GEOM table',
                 title      : 'Do not compute LEVENING_GEOM table',
                 description: 'Skip the creation of this table. </br> </br> ' +
                         '&#128736; Default value: <b>false </b>',
                 min        : 0, max: 1,
                 type: Boolean.class
                ],
        confSkipLnight          : [
                name       : 'Skip LNIGHT_GEOM table',
                title      : 'Do not compute LNIGHT_GEOM table',
                description: 'Skip the creation of this table. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1, type: Boolean.class
        ],
        confSkipLden            : [
                name       : 'Skip LDEN_GEOM table',
                title      : 'Do not compute LDEN_GEOM table',
                description: 'Skip the creation of this table. </br> </br>' +
                        '&#128736; Default value : <b> false </b>',
                min        : 0, max: 1,
                type: Boolean.class
        ],
        confExportSourceId      : [
                name       : 'keep source id',
                title      : 'Separate receiver level by source identifier',
                description: 'Keep source identifier in output in order to get noise contribution of each noise source. </br> </br>' +
                        '&#128736; Default value: <b> false </b>',
                min        : 0, max: 1,
                type: Boolean.class
        ],
        confHumidity            : [
                name       : 'Relative humidity',
                title      : 'Relative humidity',
                description: '&#127783; Humidity for noise propagation. </br> </br>' +
                        '&#128736; Default value: <b> 70</b>',
                min        : 0, max: 1,
                type: Double.class
        ],
        confTemperature         : [
                name       : 'Temperature',
                title      : 'Air temperature',
                description: '&#127777; Air temperature in degree celsius. </br> </br>' +
                        '&#128736; Default value: <b> 15</b>',
                min        : 0, max: 1,
                type: Double.class
        ],
        confFavorableOccurrencesDay: [
                name       : 'Probability of occurrences (Day)',
                title      : 'Probability of occurrences (Day)',
                description: 'Comma-delimited string containing the probability of occurrences of favourable propagation conditions. </br> </br>' +
                        'The north slice is the last array index not the first one <br/>' +
                        'Slice width are 22.5&#176;: (16 slices)<br/><ul>' +
                        '<li>The first column 22.5&#176; contain occurrences between 11.25 to 33.75 &#176;</li>' +
                        '<li>The last column 360&#176; contains occurrences between 348.75&#176; to 360&#176; and 0 to 11.25&#176;</li></ul>' +
                        '&#128736; Default value: <b>0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5</b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confFavorableOccurrencesEvening: [
                name       : 'Probability of occurrences (Evening)',
                title      : 'Probability of occurrences (Evening)',
                description: 'Comma-delimited string containing the probability of occurrences of favourable propagation conditions. </br> </br>' +
                        'The north slice is the last array index not the first one <br/>' +
                        'Slice width are 22.5&#176;: (16 slices)<br/><ul>' +
                        '<li>The first column 22.5&#176; contain occurrences between 11.25 to 33.75 &#176;</li>' +
                        '<li>The last column 360&#176; contains occurrences between 348.75&#176; to 360&#176; and 0 to 11.25&#176;</li></ul>' +
                        '&#128736; Default value: <b>0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5</b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confFavorableOccurrencesNight: [
                name       : 'Probability of occurrences (Night)',
                title      : 'Probability of occurrences (Night)',
                description: 'Comma-delimited string containing the probability of occurrences of favourable propagation conditions. </br> </br>' +
                        'The north slice is the last array index not the first one <br/>' +
                        'Slice width are 22.5&#176;: (16 slices)<br/><ul>' +
                        '<li>The first column 22.5&#176; contain occurrences between 11.25 to 33.75 &#176;</li>' +
                        '<li>The last column 360&#176; contains occurrences between 348.75&#176; to 360&#176; and 0 to 11.25&#176;</li></ul>' +
                        '&#128736; Default value: <b>0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5</b>',
                min        : 0, max: 1,
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

void exportDomain(LDENPropagationProcessData inputData, String path, int epsg) {
    System.println("Export domain : Cell number " + inputData.cellId.toString())
    FileOutputStream outData = new FileOutputStream(path)
    KMLDocument kmlDocument = new KMLDocument(outData)
    kmlDocument.setInputCRS("EPSG:" + epsg.toString())
    kmlDocument.writeHeader();
    kmlDocument.setOffset(new Coordinate(0, 0, 0))
    kmlDocument.writeTopographic(inputData.freeFieldFinder.getTriangles(), inputData.freeFieldFinder.getVertices())
    kmlDocument.writeBuildings(inputData.freeFieldFinder)
    kmlDocument.writeFooter()
}

def forgeCreateTable(Sql sql, String tableName, LDENConfig ldenConfig, LDENConfig.TIME_PERIOD period, String geomField, String tableReceiver, String tableResult) {
    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    StringBuilder sb = new StringBuilder("create table ");
    sb.append(tableName);
    if (!ldenConfig.mergeSources) {
        sb.append(" (IDRECEIVER bigint NOT NULL");
        sb.append(", IDSOURCE bigint NOT NULL");
    } else {
        sb.append(" (IDRECEIVER bigint NOT NULL");
    }
    sb.append(", THE_GEOM geometry")
    for (int idfreq = 0; idfreq < ldenConfig.getPropagationProcessPathData(period).freq_lvl.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(ldenConfig.getPropagationProcessPathData(period).freq_lvl.get(idfreq));
        sb.append(" numeric(5, 2)");
    }
    sb.append(", LAEQ numeric(5, 2), LEQ numeric(5, 2) ) AS SELECT PK");
    if (!ldenConfig.mergeSources) {
        sb.append(", IDSOURCE");
    }
    sb.append(", ")
    sb.append(geomField)
    for (int idfreq = 0; idfreq < ldenConfig.getPropagationProcessPathData(period).freq_lvl.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(ldenConfig.getPropagationProcessPathData(period).freq_lvl.get(idfreq));
    }
    sb.append(", LAEQ, LEQ FROM ")
    sb.append(tableReceiver)
    if (!ldenConfig.mergeSources) {
        // idsource can't be null so we can't left join
        sb.append(" a, ")
        sb.append(tableResult)
        sb.append(" b WHERE a.PK = b.IDRECEIVER")
    } else {
        sb.append(" a LEFT JOIN ")
        sb.append(tableResult)
        sb.append(" b ON a.PK = b.IDRECEIVER")
    }
    sql.execute(sb.toString())
    // apply pk
    logger.info("Add primary key on " + tableName)
    if (!ldenConfig.mergeSources) {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER, IDSOURCE)")
    } else {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER)")
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
    //sql.execute(String.format("UPDATE metadata SET road_conf =" + input.confId))
    //sql.execute(String.format("UPDATE metadata SET road_start = NOW();"))

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

    boolean export = false
    String pathOutput = "C:\\Users\\aumond\\Documents\\Logiciels\\NoiseModelling_3.3.2\\NoiseModelling_3.4.4" // if true change this repertory

    String sources_table_name = "LW_ROADS"

    // Pointing the 'receivers' table
    String receivers_table_name = "receivers"
    // do it case-insensitive
    receivers_table_name = receivers_table_name.toUpperCase()
    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)
    List<String> geomFieldsRcv = GeometryTableUtilities.getGeometryColumnNames(connection, TableLocation.parse(receiverTableIdentifier.toString(), dbTypes))
    if (geomFieldsRcv.isEmpty()) {
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier))
    }
    // Check if srid are in metric projection and are all the same.
    int sridReceivers = GeometryTableUtilities.getSRID(connection, TableLocation.parse(receivers_table_name))
    if (sridReceivers == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+receivers_table_name+".")
    if (sridReceivers == 0) throw new IllegalArgumentException("Error : The table "+receivers_table_name+" does not have an associated SRID.")


    // Get the primary key field of the receiver table
    int pkIndexRecv = JDBCUtilities.getIntegerPrimaryKey(connection, receiverTableIdentifier)
    if (pkIndexRecv < 1) {
        throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", receiverTableIdentifier))
    }

    // Pointing the 'buildings' table
    String building_table_name = "building"
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

    // Pointing the 'landcover' table
    String ground_table_name = "ground_acoustic"
    // do it case-insensitive
    ground_table_name = ground_table_name.toUpperCase()
    // Check if srid are in metric projection and are all the same.
    int sridGROUND = GeometryTableUtilities.getSRID(connection, TableLocation.parse(ground_table_name))
    if (sridGROUND == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+ground_table_name+".")
    if (sridGROUND == 0) throw new IllegalArgumentException("Error : The table "+ground_table_name+" does not have an associated SRID.")

    // -----------------------------------------------------------------------------
    // Define and set the parameters coming from the global configuration table (CONF)
    // The parameters are selected using the confId input variable

    /*
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
     */

    //def row_conf = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", input.confId)
    int reflexion_order = 0
    if (input['confReflOrder']) {
        reflexion_order = Integer.valueOf(input['confReflOrder'])
    }

    int max_src_dist = 150
    if (input['confMaxSrcDist']) {
        max_src_dist = Integer.valueOf(input['confMaxSrcDist'])
    }

    int max_ref_dist = 50
    if (input['confMaxReflDist']) {
        max_ref_dist = Integer.valueOf(input['confMaxReflDist'])
    }

    double wall_alpha = 0.1
    if (input['paramWallAlpha']) {
        wall_alpha = Double.valueOf(input['paramWallAlpha'])
    }

    int n_thread = 0
    if (input['confThreadNumber']) {
        n_thread = Integer.valueOf(input['confThreadNumber'])
    }

    boolean compute_vertical_diffraction = false
    if (input['confDiffVertical']) {
        compute_vertical_diffraction = input['confDiffVertical']
    }

    boolean compute_horizontal_diffraction = false
    if (input['confDiffHorizontal']) {
        compute_horizontal_diffraction = input['confDiffHorizontal']
    }

    boolean confSkipLday = false;
    if (input['confSkipLday']) {
        confSkipLday = input['confSkipLday']
    }

    boolean confSkipLevening = false;
    if (input['confSkipLevening']) {
        confSkipLevening = input['confSkipLevening']
    }

    boolean confSkipLnight = false;
    if (input['confSkipLnight']) {
        confSkipLnight = input['confSkipLnight']
    }

    boolean confSkipLden = false;
    if (input['confSkipLden']) {
        confSkipLden = input['confSkipLden']
    }

    boolean confExportSourceId = false;
    if (input['confExportSourceId']) {
        confExportSourceId = input['confExportSourceId']
    }
    // overwrite with the system number of thread - 1
    Runtime runtime = Runtime.getRuntime();
    n_thread = Math.max(1, runtime.availableProcessors() - 1)


    //logger.info(String.format("PARAM : You have chosen the configuration number %d ", input.confId));
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

    def row_zone = sql.firstRow("SELECT * FROM DEM")

    PropagationProcessPathData environmentalDataDay = new PropagationProcessPathData(false)

    double confHumidity = 75.6
    if (input.containsKey('confHumidity')) {
        environmentalDataDay.setHumidity(input['confHumidity'] as Double)
    }
    double confTemperature = 10.6
    if (input.containsKey('confTemperature')) {
        environmentalDataDay.setTemperature(input['confTemperature'] as Double)
    }

    logger.info(String.format("PARAM : The relative humidity is set to %s ", confHumidity));
    logger.info(String.format("PARAM : The temperature is set to %s ", confTemperature));

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
    ldenConfig_propa.setlDayTable("LDAY_ROADS_RESULT")
    ldenConfig_propa.setlEveningTable("LEVENING_ROADS_RESULT")
    ldenConfig_propa.setlNightTable("LNIGHT_ROADS_RESULT")
    ldenConfig_propa.setlDenTable("LDEN_ROADS_RESULT")
    ldenConfig_propa.setComputeLAEQOnly(false)

    // --------------------------------------------
    // Run Calculations
    // --------------------------------------------

    int maximumRaysToExport = 5000

    File folderExportKML = null
    String kmlFileNamePrepend = ""
    if (input['confRaysName'] && !((input['confRaysName'] as String).isEmpty())) {
        String confRaysName = input['confRaysName'] as String
        if(confRaysName.startsWith("file:")) {
            ldenConfig_propa.setExportRaysMethod(LDENConfig.ExportRaysMethods.TO_MEMORY)
            URL url = new URL(confRaysName)
            File urlFile = new File(url.toURI())
            if(urlFile.isDirectory()) {
                folderExportKML = urlFile
            } else {
                folderExportKML = urlFile.getParentFile()
                kmlFileNamePrepend = confRaysName.substring(
                        Math.max(0, confRaysName.lastIndexOf(File.separator) + 1),
                        Math.max(0, confRaysName.lastIndexOf(".")))
            }
        } else {
            ldenConfig_propa.setExportRaysMethod(LDENConfig.ExportRaysMethods.TO_RAYS_TABLE)
            ldenConfig_propa.setRaysTable(input['confRaysName'] as String)
        }
        ldenConfig_propa.setKeepAbsorption(true);
        ldenConfig_propa.setMaximumRaysOutputCount(maximumRaysToExport);
    }

    LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa)
    // Add train directivity
    // TODO add optional discrete directivity table name
    ldenProcessing.insertTrainDirectivity()
    pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
    pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
    pointNoiseMap.setSoundReflectionOrder(reflexion_order)

    // Set environmental parameters


    environmentalDataDay.setHumidity(confHumidity)
    environmentalDataDay.setTemperature(confTemperature)

    PropagationProcessPathData environmentalDataEvening = new PropagationProcessPathData(environmentalDataDay)
    PropagationProcessPathData environmentalDataNight = new PropagationProcessPathData(environmentalDataDay)
    if (input.containsKey('confFavorableOccurrencesDay')) {
        StringTokenizer tk = new StringTokenizer(input['confFavorableOccurrencesDay'] as String, ',')
        double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length]
        for (int i = 0; i < favOccurrences.length; i++) {
            favOccurrences[i] = Math.max(0, Math.min(1, Double.valueOf(tk.nextToken().trim())))
        }
        environmentalDataDay.setWindRose(favOccurrences)
    }
    if (input.containsKey('confFavorableOccurrencesEvening')) {
        StringTokenizer tk = new StringTokenizer(input['confFavorableOccurrencesEvening'] as String, ',')
        double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length]
        for (int i = 0; i < favOccurrences.length; i++) {
            favOccurrences[i] = Math.max(0, Math.min(1, Double.valueOf(tk.nextToken().trim())))
        }
        environmentalDataEvening.setWindRose(favOccurrences)
    }
    if (input.containsKey('confFavorableOccurrencesNight')) {
        StringTokenizer tk = new StringTokenizer(input['confFavorableOccurrencesNight'] as String, ',')
        double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length]
        for (int i = 0; i < favOccurrences.length; i++) {
            favOccurrences[i] = Math.max(0, Math.min(1, Double.valueOf(tk.nextToken().trim())))
        }
        environmentalDataNight.setWindRose(favOccurrences)
    }

    logger.info(String.format("PARAM : The favorable occurrences of day is set to %s ", environmentalDataDay.getWindRose().toString()));
    logger.info(String.format("PARAM : The favorable occurrences of evening is set to %s ", environmentalDataEvening.getWindRose().toString()));
    logger.info(String.format("PARAM : The favorable occurrences of night is set to %s ", environmentalDataNight.getWindRose().toString()));

    pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.DAY, environmentalDataDay)
    pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.EVENING, environmentalDataEvening)
    pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.NIGHT, environmentalDataNight)

    ldenConfig_propa.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.DAY, environmentalDataDay)
    ldenConfig_propa.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.EVENING, environmentalDataEvening)
    ldenConfig_propa.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.NIGHT, environmentalDataNight)

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

    pointNoiseMap.setGridDim(100)
    logger.info("Taille de cellulle : " + pointNoiseMap.getCellWidth().toString())

    // Init Map
    pointNoiseMap.initialize(connection, new EmptyProgressVisitor())

    logger.info("Start calculation... ")

    ProgressVisitor progressLogger

    if("progressVisitor" in input) {
        progressLogger = input["progressVisitor"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1);
    }

    String profileName = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy_MM_dd.HH'h'mm'm'ss's'", Locale.ROOT))+"."+System.currentTimeMillis();
    File profileFile
    if("workingDirectory" in input) {
        profileFile = new File(new File(input["workingDirectory"] as String), "profile_"+profileName+".csv")
        ldenConfig_propa.setSqlOutputFile(new File(new File(input["workingDirectory"] as String), "Road_Noise_level.sql.gz"))
        ldenConfig_propa.setSqlOutputFileCompression(true)
    } else {
        profileFile = new File("profile_"+profileName+".csv")
        ldenConfig_propa.setSqlOutputFile(new File("Road_Noise_level.sql.gz"))
        ldenConfig_propa.setSqlOutputFileCompression(true)
    }

    ProfilerThread profilerThread = new ProfilerThread(profileFile);
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
                        ldenPropagationProcessData.profileBuilder.getBuildingCount()));
                if (export) {
                     logger.info(String.format("Export Domain : ") + String.format("Domain_part_%d.kml", k))
                    exportDomain(ldenPropagationProcessData,pathOutput.toString() + "\\" + String.format("Domain_part_%d.kml", k),sridBuildings)
                    }
            }
        }
    } catch(IllegalArgumentException | IllegalStateException ex) {
        System.err.println(ex);
        throw ex;
    } finally {
        profilerThread.stop();
        ldenProcessing.stop()
    }

    //logger.info("Table(s) " + createdTables.toString() + " have been created and saved into the Road_Noise_level.sql.gz file.")


    // ---------------------------------------------------------
    // Start the upload into the NM db
    logger.info('Start uploading the Road_Noise_level.sql.gz file into NoiseModelling.')

    File scriptFile = new File("Road_Noise_level.sql.gz")
    if("workingDirectory" in input) {
        scriptFile = new File(new File(input["workingDirectory"] as String), "Road_Noise_level.sql.gz")
    }
    if(!scriptFile.exists()) {
        logger.info("NON")
        return scriptFile.absolutePath + " does not exists"
    }

    parseScript(scriptFile, sql, progressLogger, true)

    // Associate Geometry column to the table LDEN
    StringBuilder createdTables = new StringBuilder()

    if (ldenConfig_propa.computeLDay) {
        sql.execute("drop table if exists LDAY_ROADS;")
        logger.info('create table LDAY_ROADS')
        forgeCreateTable(sql, "LDAY_ROADS", ldenConfig_propa, LDENConfig.TIME_PERIOD.DAY, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig_propa.lDayTable)
        createdTables.append(" LDAY_ROADS")
        sql.execute("drop table if exists " + TableLocation.parse(ldenConfig_propa.getlDayTable()))
    }
    if (ldenConfig_propa.computeLEvening) {
        sql.execute("drop table if exists LEVENING_ROADS;")
        logger.info('create table LEVENING_ROADS')
        forgeCreateTable(sql, "LEVENING_ROADS", ldenConfig_propa, LDENConfig.TIME_PERIOD.EVENING, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig_propa.lEveningTable)
        createdTables.append(" LEVENING_ROADS")
        sql.execute("drop table if exists " + TableLocation.parse(ldenConfig_propa.getlEveningTable()))
    }
    if (ldenConfig_propa.computeLNight) {
        sql.execute("drop table if exists LNIGHT_ROADS;")
        logger.info('create table LNIGHT_ROADS')
        forgeCreateTable(sql, "LNIGHT_ROADS", ldenConfig_propa, LDENConfig.TIME_PERIOD.NIGHT, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig_propa.lNightTable)
        createdTables.append(" LNIGHT_ROADS")
        sql.execute("drop table if exists " + TableLocation.parse(ldenConfig_propa.getlNightTable()))
    }
    if (ldenConfig_propa.computeLDEN) {
        sql.execute("drop table if exists LDEN_ROADS;")
        logger.info('create table LDEN_ROADS')
        forgeCreateTable(sql, "LDEN_ROADS", ldenConfig_propa, LDENConfig.TIME_PERIOD.DAY, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig_propa.lDenTable)
        createdTables.append(" LDEN_ROADS")
        sql.execute("drop table if exists " + TableLocation.parse(ldenConfig_propa.getlDenTable()))
    }

    //sql.execute(String.format("UPDATE metadata SET road_end = NOW();"))
    resultString = "Process done! Table(s) " + createdTables.toString() + " have been uploaded into NoiseModelling."
    // print to command window
    logger.info('Result : ' + resultString)
    // print to WPS Builder
    return resultString
}


@CompileStatic
static def parseScript(File scriptFile, Sql sql, ProgressVisitor progressLogger, boolean compressed) {
    long scriptFileSize = Files.size(scriptFile.toPath())
    int BUFFER_LENGTH = 65536
    ProgressVisitor subProgress = progressLogger.subProcess((int)(scriptFileSize / BUFFER_LENGTH))
    Reader reader = null
    try {
        FileInputStream s = new FileInputStream(scriptFile)
        InputStream is = s
        if(compressed) {
            is = new GZIPInputStream(s, BUFFER_LENGTH)
        }
        reader  = new BufferedReader(new InputStreamReader(is))
        ScriptReader scriptReader = new ScriptReader(reader)
        String statement = scriptReader.readStatement()
        while (statement != null) {
            sql.execute(statement)
            subProgress.setStep((int)(s.getChannel().position() / BUFFER_LENGTH))
            statement = scriptReader.readStatement()
        }
    } finally {
        reader.close()
    }
}
