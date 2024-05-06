/**
 * TODO
 */

/*
<dependency>
            <groupId>org.geoserver</groupId>
            <artifactId>gs-main</artifactId>
            <version>2.24.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.geotools</groupId>
            <artifactId>gt-main</artifactId>
            <version>29.1</version>
        </dependency>
 */

//import geoserver.GeoServer
//import geoserver.catalog.Store
//import org.geotools.jdbc.JDBCDataStore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.io.FilenameUtils
import org.noise_planet.noisemodelling.jdbc.*
import org.noise_planet.noisemodelling.pathfinder.*
import org.noise_planet.noisemodelling.emission.*
import org.noise_planet.noisemodelling.propagation.*

title = 'Export files for generate à noise map'

description = '&#10145;&#65039; Use the files export by geoClimate for create files the noise of the location. This return <b>.geojson</b> files </br>' +
        '<img src="/wps_images/import_osm_file.png" alt="Import OSM file" width="95%" align="center">'

inputs = [
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

/*
// Open Connection to Geoserver
static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}
*/

run(inputs)

// run the script
def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    //String dbName = "h2gisdb"

    // Open connection
    /*
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
    */
    exec(input)
}

// main function of the script
def exec(/*Connection connection,*/ input) {


    //Map buildingsParamsMap = buildingsParams.toMap();
    //connection = new ConnectionWrapper(connection)

    //Sql sql = new Sql(connection)

    String resultString

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info('Start : Create files from GéoClimate')
    logger.info("inputs {}", input)

    // -------------------
    // Get every inputs
    // -------------------

    String inputDirectory = System.getProperty("user.dir")+"\\..\\outPut\\geoClimate\\osm_Orbey" /*input["filesImportPath"] as String*/
    String outputDirectory = System.getProperty("user.dir")+"\\..\\outPut\\noiseModelling" /*input["filesExportPath"] as String*/

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

    /* IN THE WPS SCRIPT

    // list of the system tables
    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

    if (areYouSure) {
        // Build the result string with every tables
        StringBuilder sb = new StringBuilder()

        // Get every table names
        List<String> tables = JDBCUtilities.getTableNames(connection, null, "PUBLIC", "%", null)
        // Loop over the tables
        tables.each { t ->
            TableLocation tab = TableLocation.parse(t)
            if (!ignorelst.contains(tab.getTable())) {
                // Add the name of the table in the string builder
                if (sb.size() > 0) {
                    sb.append(" || ")
                }
                sb.append(tab.getTable())

                // Create a connection statement to interact with the database in SQL
                Statement stmt = connection.createStatement()
                // Drop the table
                stmt.execute("drop table if exists " + tab)
            }
        }
        resultString = "The table(s) " + sb.toString() + " was/were dropped."
    } else {
        resultString = "If you're not sure, we won't do anything !"
    }*/

    def folder = new File(inputDirectory)
    def tablesName = []


    folder.eachFile { file ->
        if (file.isFile() && file.name.endsWith('.geojson')) {

            // get the name of the fileName
            String fileName = FilenameUtils.removeExtension(new File(inputDirectory+file).getName())
            // replace whitespaces by _ in the file name
            fileName.replaceAll("\\s", "_")
            // remove special characters in the file name
            fileName.replaceAll("[^a-zA-Z0-9 ]+", "_")
            // the tableName will be called as the fileName
            tablesName.add(fileName.toUpperCase())

        }
    }

    println("Fichiers .geojson trouvés : $tablesName")


    resultString = "Success"

    logger.info('End : All files are get')
    logger.info('Result : ' + resultString)
    return resultString
}

/*
<dependency>
    <groupId>org.matsim</groupId>
    <artifactId>matsim</artifactId>
    <version>11.0</version>
    <exclusions>
        <exclusion>
            <groupId>org.geotools</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
 */
