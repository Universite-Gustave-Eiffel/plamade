/**
 * TODO
 * !! NOT WORK !!
 */

import groovy.sql.Sql
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.geojson.GeoJsonDriverFunction
import org.h2gis.functions.io.geojson.GeoJsonWrite
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.io.FilenameUtils
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.nio.file.Paths

title = 'Export files to generate a noise map'

description = '&#10145;&#65039; Use the files export by geoClimate for create files the noise of the location. This return <b>.geojson</b> files </br>' +
        '<img src="/wps_images/import_osm_file.png" alt="Import OSM file" width="95%" align="center">'

inputs = [
        locations : [
                name       : 'Name of the municipality or street',
                title      : 'Name of the location',
                description: '&#128194; Name of the municipality or street you want informations.<br>' +
                        'For example: Paris',
                type       : String.class
        ],
        filesImportPath : [
                name       : 'Files import path',
                title      : 'Files import path',
                description: '&#128194; Path of the directory you have export geoClimates files (like buildings.geojson).<br>' +
                        'For example: C:\\Home\\GeoClimate\\Output',
                type       : String.class
        ],
        filesExportPath   : [
                name:        'Files export path',
                title:       'Files export path',
                description: '&#128194; Path of the directory you want to export the files created by noiseModelling </br> </br>' +
                        'For example: C:\\Home\\NoiseModelling\\Output',
                type: String.class
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

    Connection connection
    connection = null

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

    def connection = openH2GISDataStoreConnection(dbName);
    connection.withCloseable {
        conn ->
            return [result: exec(conn, input)]
    }

}

// main function of the script
def exec(Connection connection, input) {


    //Map buildingsParamsMap = buildingsParams.toMap();
    def newConnection = new ConnectionWrapper(connection)

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

    String inputDirectory = Paths.get(System.getProperty("user.dir"), "..", "..", "..", "outPut", "geoClimate", "osm_${location}").toString() /*input["filesImportPath"] as String*/
    String outputDirectory = Paths.get(System.getProperty("user.dir"), "..", "..", "..", "outPut", "noiseModelling").toString() /*input["filesExportPath"] as String*/


    try {

        if(!inputDirectory){
            throw new IllegalArgumentException('ERROR : The input directory to take files with datas cannot be null or empty')
        }

        // Test if the input folder exist
        File inputFiles = new File(inputDirectory)
        if(!inputFiles.exists() || !inputFiles.isDirectory()){
            throw new IllegalArgumentException("ERROR : The input directory doesn't exist. The data can't be take")
        }

        if(!outputDirectory){
            throw new IllegalArgumentException('ERROR : The output directory to store the result cannot be null or empty')
        }

        // Test if the outPut folder exist
        File outputFiles = new File(outputDirectory)
        if(!outputFiles.exists() || !outputFiles.isDirectory()){
            logger.info("Create the output directory because it doesn't exist")
            outputFiles.mkdir()
        }

    } catch (IllegalArgumentException e) {
        return e.toString()
    }

    /* IN THE WPS SCRIPT --> TODO

    // list of the system tables
    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

    if (areYouSure) {
        // Build the result string with every tables
        StringBuilder sb = new StringBuilder()

        // Get every table names
        List<String> tables = JDBCUtilities.getTableNames(newConnection, null, "PUBLIC", "%", null)
        // Loop over the tables
        tables.each { t ->
            TableLocation tab = TableLocation.parse(t)
            if (!ignorelst.contains(tab.getTable())) {
                // Add the name of the table in the string builder
                if (sb.size() > 0) {
                    sb.append(" || ")
                }
                sb.append(tab.getTable())

                // Create a newConnection statement to interact with the database in SQL
                Statement stmt = newConnection.createStatement()
                // Drop the table
                stmt.execute("drop table if exists " + tab)
            }
        }
        resultString = "The table(s) " + sb.toString() + " was/were dropped."
    } else {
        resultString = "If you're not sure, we won't do anything !"
    }*/

    Statement stmt = newConnection.createStatement()

    def folderin = new File(inputDirectory)
    def tablesName = []


    folderin.eachFile { file ->
        if (file.isFile() && file.name.endsWith('.geojson')) {

            // get the name of the fileName
            String fileName = FilenameUtils.removeExtension(new File(inputDirectory+file).getName())
            // replace whitespaces by _ in the file name
            fileName.replaceAll("\\s", "_")
            // remove special characters in the file name
            fileName.replaceAll("[^a-zA-Z0-9 ]+", "_")
            // the tableName will be called as the fileName
            tablesName.add((fileName+location).toUpperCase())

            println(outputDirectory)

            println(fileName.toUpperCase())

            String dropOutputTable = "drop table if exists " + (fileName+location).toUpperCase()
            stmt.execute(dropOutputTable)

            GeoJsonDriverFunction geoJsonDriver = new GeoJsonDriverFunction()
            geoJsonDriver.importFile(newConnection, (fileName+"${location}").toUpperCase(), new File(file as String), new EmptyProgressVisitor())
            //GeoJsonRead.importTable(connection,, (fileName+"${location}").toUpperCase(),null,true)

        }
    }


    tablesName.each { table ->
        GeoJsonWrite.exportTable(newConnection, Paths.get(outputDirectory, (table as String).toLowerCase() + ".geojson").toString(), "(SELECT * FROM ${table})")
    }

    println("Fichiers .geojson trouvés : $tablesName")


    resultString = "Success"

    logger.info('End : All files are get')
    logger.info('Result : ' + resultString)
    return resultString
}

/*
<dependency>
    <groupId>org.orbisgis</groupId>
    <artifactId>noisemodelling-emission</artifactId>
    <version>4.0.6-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>org.orbisgis</groupId>
    <artifactId>noisemodelling-propagation</artifactId>
    <version>4.0.6-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>org.orbisgis</groupId>
    <artifactId>noisemodelling-pathfinder</artifactId>
    <version>4.0.6-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>org.orbisgis</groupId>
    <artifactId>noisemodelling-jdbc</artifactId>
    <version>4.0.6-SNAPSHOT</version>
</dependency>
 */
