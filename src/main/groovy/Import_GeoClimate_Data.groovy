/**
 * TODO
 */
/**
 * INFO : The snapshot version of geoClimate use is not the good but the 1.0.1 generate an error.
 */


//import geoserver.GeoServer
import groovy.sql.Sql
//import org.geoserver.*
//import org.geotools.data.store.*
//import geoserver.catalog.Store
//import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.orbisgis.geoclimate.osm.OSM
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import utils.RoadValue
import java.sql.DriverManager;

import java.sql.Connection

title = 'Import building, ground_acoustic, road_traffic, rail and zone files from GéoClimate'

description = '&#10145;&#65039; Use a location (town or street) and geoClimate lib for get data about this location. This return <b>.geojson</b> files </br>' +
        '<hr>' +
        'The following output .geojson will be created: <br>' +
        '- <b> building </b>: a file containing the buildings <br>' +
        '- <b> ground_acoustic </b>: a file containing ground acoustic absorption. <br>' +
        '- <b> road_traffic </b>: a file containing the roads. <br>' +
        '- <b> rail </b>: a file containing the railway. <br>' +
        '- <b> zone </b>: a file containing the studied area. <br>' +
        /*'&#128161; The user can choose to avoid creating some of these tables by checking the dedicated boxes </br> </br>' +*/
        '<img src="/wps_images/import_osm_file.png" alt="Import OSM file" width="95%" align="center">'

inputs = [
        locations : [
                name       : 'Name of the municipality or street',
                title      : 'Name of the location',
                description: '&#128194; Name of the municipality or street you want informations.<br>' +
                        'For example: Paris',
                type       : String.class
        ],
        filesExportPath   : [
                name:        'Files export path',
                title:       'Files export path',
                description: '&#128194; Path of the directory you want to export the files created by géoClimate </br> </br>' +
                        'For example: C:\\Home\\GeoClimate\\Output',
                type: String.class
        ],
        targetSRID : [
                name       : 'Target projection identifier',
                title      : 'Target projection identifier',
                description: '&#127757; Target projection identifier (also called SRID) of your table.<br>' +
                        'It should be an <a href="https://epsg.io/" target="_blank">EPSG</a> code, an integer with 4 or 5 digits (ex: <a href="https://epsg.io/3857" target="_blank">3857</a> is Web Mercator projection).<br><br>' +
                        '&#x1F6A8; The target SRID must be in <b>metric</b> coordinates. </br>' +
                        '&#128736; Default value: <b>2154 </b> ',
                min        : 0, max: 1,
                type       : Integer.class
        ],
        geoclimatedb: [
                name       : 'Temporary database',
                title      : 'Temporary database',
                description: '&#127757; Database use by géoClimate for create files (is a .mv.db) <br>' +
                        'if you want keep this file, uncheck the button' +
                        '&#128736; Default value: <b> true </b> ',
                min        : 0, max: 1,
                type       : Boolean.class
        ],
]

outputs = [
        result : [
                name: 'Result output string',
                title: 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type: String.class
        ]
]

// Open Connection to H2GIS Database
static Connection openH2GISDataStoreConnection(String dbName) {
  // Driver class name for H2GIS
  String driverClassName = "org.h2.Driver";
  // JDBC URL for H2GIS (change it as needed)
  String jdbcUrl = "jdbc:h2:~/"+dbName;

  Connection connection = null;
  try {
    // Load JDBC driver
    Class.forName(driverClassName);
    // Establish connection
    connection = DriverManager.getConnection(jdbcUrl);
  } catch (Exception e) {
    e.printStackTrace();
  }
  return connection;
}


run(inputs)

// run the script
def run(input) {

  // Get name of the database
  // by default an embedded h2gis database is created
  // Advanced user can replace this database for a postGis or h2Gis server database.
  String dbName = "h2gisdb"

  // Open connection
  def connection = openH2GISDataStoreConnection(dbName);
  connection.withCloseable {
    conn ->
      return [result: exec(conn, input)]
  }

  //exec(input)
}

// main function of the script
def exec(Connection connection, input) {


  //Map buildingsParamsMap = buildingsParams.toMap();
  connection = new ConnectionWrapper(connection)

  Sql sql = new Sql(connection)

  String resultString

  Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
  logger.info('Start : Create files from GéoClimate')
  logger.info("inputs {}", input)

  // -------------------
  // Get every inputs
  // -------------------

  String location = "Epfig"/*input["locations"] as String
  if(location.isEmpty() || !input["locations"]){
      resultString = "Location argument has not been provided."
      throw new Exception('ERROR : ' + resultString)
  }*/

  String outputDirectory = System.getProperty("user.dir")+"\\..\\outPut\\geoClimate" /*input["filesExportPath"] as String*/

  try{

    if(!outputDirectory){
      throw new IllegalArgumentException('ERROR : The output directory to store the result cannot be null or empty')
    }

    // Test if the outPut folder exist
    File dirFile = new File(outputDirectory)
    if(!dirFile.exists() || !dirFile.isDirectory()){
      logger.info("Create the output directory because it doesn't exist")
      dirFile.mkdir()
    }
  } catch (IllegalArgumentException e){
    return e.toString()
  }

  Integer srid = 2154
  /*
  if (input['targetSRID']) {
      srid = input['targetSRID'] as Integer
  }*/

    Boolean geoclimatedb = true
    /*
    if (!input['geoclimatedb']) {
        geoclimatedb = input['geoclimatedb'] as Boolean
    }*/

  logger.info('Input Read done')

  runGeoClimate(createGeoClimateConfig(location, outputDirectory,srid, geoclimatedb, logger), logger)

  logger.info('Parse road value for noiseModelling input')

  parseRoadData(outputDirectory, location)

  logger.info('Parse road data done')

  logger.info('Parse building value for noiseModelling input')

  parseBuildingData(outputDirectory, location)

  logger.info('Parse building data done')


  logger.info('File is ready for noiseModelling')


  resultString = "Success"

  logger.info('End : All files are get')
  logger.info('Result : ' + resultString)
  return resultString
}

/**
 * Use input data to configure geoClimate input:
 * @param zone: String. The name of the chosen location.
 * @param outputDirectory: String. The location of the output files.
 * @param srid: Integer. The spatial reference identifier of the location.
 * @param logger: Logger. Displays messages in the console.
 * @return workflow_parameters: LinkedHashMap<String, Serializable>. Configure parameters for geoClimate.
 */
static def createGeoClimateConfig(String zone, String outputDirectory, Integer srid, Boolean geoclimatedb, Logger logger){

  logger.info('Creation of the config ')

  //Name of default H2GIS database
  def local_database_name="osm_geoclimate_${System.currentTimeMillis()}"

  //Set input parameters for géoClimate
  def geoInput = ["locations" : [zone], "delete":true]


  //Set output parameters for géoClimate
  def geoOutput  = [
          "folder" : [ "path": "$outputDirectory", "tables": ["building", "road_traffic", "ground_acoustic", "rail", "zone"], "srid": "$srid"]
  ]

  //Set configurations parameters
  def workflow_parameters = [
          "description" :"Run the Geoclimate chain  and export result to a folder",
          "geoclimatedb" : [
                  "folder" :outputDirectory,
                  "name" : "${local_database_name};AUTO_SERVER=TRUE",
                  "delete" : geoclimatedb,
          ],
          "input" :geoInput,
          "output" : geoOutput,
          "parameters": [
                  "rsu_indicators":[
                          "indicatorUse": ["LCZ"],
                          "estimateHeight": true,
                  ],"worldpop_indicators" : true,
                  "road_traffic" : true,
                  "noise_indicators": [
                          "ground_acoustic": true
                  ]
          ]
  ]

  logger.info('Create config file Read done')

  return workflow_parameters

}

/**
 * Call géoClimate lib with the parameters set before
 * @param workflow_parameters: LinkedHashMap<String, Serializable>. Parameters given to geoClimate
 * @param logger: Logger. Displays messages in the console.
 * @return None
 */
static def runGeoClimate( LinkedHashMap<String, Serializable> workflow_parameters, Logger logger){
  logger.info('Start import data')

  //Call géoClimate lib with configurations
  OSM.workflow(workflow_parameters)

  logger.info('Import files done')
}


/**
 * Format the name of the fields given by geoClimate so that noiseModelling can use them.
 * @param outputDirectory: String. The location of the output files.
 * @param location: String. The name of the chosen location.
 * @return None
 */
static def parseRoadData(String outputDirectory, String location){

  //Define the file to change data
  def jsonSlurper = new JsonSlurper()
  def jsonData = jsonSlurper.parse(new File(outputDirectory+"\\osm_"+location+"\\road_traffic.geojson"))

  //Loops through all "features" data in the file
  jsonData.features.each { feature ->

    def propertiesData = feature.properties
    def updatedProperties = [:]

    propertiesData.each { key, value ->
      switch (key) {
        case RoadValue.LV_D.gcProperty :
          updatedProperties[RoadValue.LV_D.nmProperty] = value
          break
        case RoadValue.EV_LV_HOUR.gcProperty :
          updatedProperties[RoadValue.EV_LV_HOUR.nmProperty] = value
          break
        case RoadValue.NIGHT_LV_HOUR.gcProperty :
          updatedProperties[RoadValue.NIGHT_LV_HOUR.nmProperty] = value
          break
        case RoadValue.DAY_LV_SPEED.gcProperty :
          updatedProperties[RoadValue.DAY_LV_SPEED.nmProperty] = value
          break
        case RoadValue.EV_LV_SPEED.gcProperty :
          updatedProperties[RoadValue.EV_LV_SPEED.nmProperty] = value
          break
        case RoadValue.NIGHT_LV_SPEED.gcProperty :
          updatedProperties[RoadValue.NIGHT_LV_SPEED.nmProperty] = value
          break
        case RoadValue.DAY_HV_HOUR.gcProperty :
          updatedProperties[RoadValue.DAY_HV_HOUR.nmProperty] = value
          break
        case RoadValue.EV_HV_HOUR.gcProperty :
          updatedProperties[RoadValue.EV_HV_HOUR.nmProperty] = value
          break
        case RoadValue.NIGHT_HV_HOUR.gcProperty :
          updatedProperties[RoadValue.NIGHT_HV_HOUR.nmProperty] = value
          break
        case RoadValue.DAY_HV_SPEED.gcProperty :
          updatedProperties[RoadValue.DAY_HV_SPEED.nmProperty] = value
          break
        case RoadValue.EV_HV_SPEED.gcProperty :
          updatedProperties[RoadValue.EV_HV_SPEED.nmProperty] = value
          break
        case RoadValue.NIGHT_HV_SPEED.gcProperty :
          updatedProperties[RoadValue.NIGHT_HV_SPEED.nmProperty] = value
          break
        case RoadValue.PAVEMENT.gcProperty :
          updatedProperties[RoadValue.PAVEMENT.nmProperty] = value
          break
        case RoadValue.DIRECTION.gcProperty :
          updatedProperties[RoadValue.DIRECTION.nmProperty] = value
          break
        default :
          updatedProperties[key] = value
      }
    }
    feature.properties = updatedProperties
  }

  def newJsonBuilder = new JsonBuilder(jsonData)
  def newJsonString = newJsonBuilder.toPrettyString()

  new File(outputDirectory+"\\osm_"+location+"\\road_traffic.geojson").text = newJsonString

}

/**
 * The building layer issued from GeoClimate is updated by adding a new attribute named 'HEIGHT' that corresponds with the already existing field 'HEIGHT_ROOF'.
 * @param outputDirectory: String. The location of the output files.
 * @param location: String. The name of the chosen location.
 * @return None
 */
static def parseBuildingData(String outputDirectory, String location){

  //Define the file to change data
  def jsonSlurper = new JsonSlurper()
  def jsonData = jsonSlurper.parse(new File(outputDirectory+"\\osm_"+location+"\\building.geojson"))

  //Loops through all "features" data in the file
  jsonData.features.each { feature ->

    def propertiesData = feature.properties
    def updatedProperties = [:]

    propertiesData.each { key, value ->
      switch (key) {
        case "HEIGHT_ROOF" :
          updatedProperties["HEIGHT"] = value
          break
        default :
          updatedProperties[key] = value
      }
    }
    feature.properties = updatedProperties
  }

  def newJsonBuilder = new JsonBuilder(jsonData)
  def newJsonString = newJsonBuilder.toPrettyString()

  new File(outputDirectory+"\\osm_"+location+"\\building.geojson").text = newJsonString

}

