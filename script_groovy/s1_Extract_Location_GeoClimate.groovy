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
 * @Author Samuel Marsault, Trainee
 */

/* TODO
   - Merge 3D lines topo with BD Alti
   - Confirm that screens are taken 2 times into account for railway
   - Check spatial index and srids
*/

package org.noise_planet.noisemodelling.wps.plamade

import groovy.io.FileType
import groovy.sql.Sql
import groovy.text.SimpleTemplateEngine
import groovy.transform.CompileStatic
import org.apache.commons.io.FilenameUtils
import org.h2.util.ScriptReader
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.geojson.GeoJsonDriverFunction
import org.h2gis.utilities.GeometryMetaData
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

title = 'Extract department'
description = 'Connect to a distant PostGIS database and extract departments according to Plamade specification'

inputs = [
        databaseUser : [
                name       : 'PostGIS user',
                title      : 'PostGIS username',
                description: 'PostGIS username for authentication',
                type       : String.class
        ],
        databasePassword : [
                name       : 'PostGIS password',
                title      : 'PostGIS password',
                description: 'PostGIS password for authentication',
                type       : String.class
        ],
        fetchDistance : [
                name       : 'Fetch distance',
                title      : 'Fetch distance',
                description: 'Fetch distance around the selected area in meters. Default 1000',
                min : 0, max: 1000,
                type       : Integer.class
        ],
        location   : [
                name       : 'Location',
                title      : 'Location',
                description: 'Location use for get the result',
                type       : String.class
        ],
        inputServer : [
                name       : 'DB Server used',
                title      : 'DB server used',
                description: 'Choose between cerema or cloud',
                type       : String.class
        ],
        srid       : [
                name: 'SRID',
                title: 'SRID',
                description: 'Spatial reference Identifier',
                type: String.class
        ],
        resultPath : [
                name: 'Path of the result',
                title: 'Path of the result',
                description: 'Path where the results are located',
                type: String.class
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

@CompileStatic
static def parseScript(String sqlInstructions, Sql sql, ProgressVisitor progressVisitor, Logger logger) {
    Reader reader = null
    ByteArrayInputStream s = new ByteArrayInputStream(sqlInstructions.getBytes())
    InputStream is = s
    List<String> statementList = new LinkedList<>()
    try {
        reader  = new InputStreamReader(is)
        ScriptReader scriptReader = new ScriptReader(reader)
        scriptReader.setSkipRemarks(true)
        String statement = scriptReader.readStatement()
        while (statement != null && !statement.trim().isEmpty()) {
            statementList.add(statement)
            statement = scriptReader.readStatement()
        }
    } finally {
        reader.close()
    }
    int idStatement = 0
    final int nbStatements = statementList.size()
    ProgressVisitor evalProgress = progressVisitor.subProcess(nbStatements)
    for(String statement : statementList) {
        logger.info(String.format(Locale.ROOT, "%d/%d %s", (idStatement++) + 1, nbStatements, statement.trim()))
        sql.execute(statement)
        evalProgress.endStep()
        if(evalProgress.isCanceled()) {
            throw new SQLException("Canceled by user")
        }
    }
}

def exec(Connection connection, input) {


    //------------------------------------------------------
    // Clean the database before starting the importation

    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

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

    //------------------------------------------------------


    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    ProgressVisitor progressVisitor

    if("progressVisitor" in input) {
        progressVisitor = input["progressVisitor"] as ProgressVisitor
    } else {
        progressVisitor = new RootProgressVisitor(1, true, 1);
    }

    ProgressVisitor progress = progressVisitor.subProcess(2)
    // print to command window
    logger.info('Start linking with PostGIS')

    // Get provided parameters
    String location = input["location"] as String


    Integer buffer = 1000
    if ('fetchDistance' in input) {
        buffer = input["fetchDistance"] as Integer
    }

    def databaseUrl
    if(input["inputServer"].equals('cerema')) {
        databaseUrl="jdbc:postgresql_h2://161.48.203.166:5432/plamade?ssl=true&sslmode=disable"
    } else if(input["inputServer"].equals('cloud')) {
        databaseUrl = "jdbc:postgresql_h2://57.100.98.126:5432/plamade?ssl=true&sslmode=disable"
    } else{
        return "Vous n'avez pas spécifié le bon nom de serveur"
    }

    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String

    def srid = "2154"
    if('srid' in input){
        srid = input["srid"] as String
    }

    def resultPath = input["resultPath"] as String

    // Declare table variables depending on the department and the projection system
    def table_bd_topo_route = "t_route_metro_corse"


    def binding = ["buffer": buffer, "databaseUrl": databaseUrl, "user": user, "pwd": pwd, "location": location, "table_bd_topo_route" : table_bd_topo_route]

    // name of the imported tables
    String outputTableName_full = ""

    // Create a connection statement to interact with the database in SQL
    Statement stmt = connection.createStatement()

    def dir = new File(resultPath+File.separator+"osm_"+location)

    def sql = new Sql(connection)

    dir.eachFileRecurse(FileType.FILES) { file ->

        String pathFile = file as String
        String ext = pathFile.substring(pathFile.lastIndexOf('.') + 1, pathFile.length())

        if (ext == "geojson") {
            // get the name of the fileName
            String fileName = FilenameUtils.removeExtension(new File(pathFile).getName())
            // replace whitespaces by _ in the file name
            fileName.replaceAll("\\s", "_")
            // remove special characters in the file name
            fileName.replaceAll("[^a-zA-Z0-9 ]+", "_")
            // the tableName will be called as the fileName
            String outputTableName = fileName.toUpperCase()
            TableLocation outputTableIdentifier = TableLocation.parse(outputTableName, DBUtils.getDBType(connection))

            // Drop the table if already exists
            String dropOutputTable = "drop table if exists " + outputTableIdentifier.toString()
            stmt.execute(dropOutputTable)

            GeoJsonDriverFunction geoJsonDriver = new GeoJsonDriverFunction()
            geoJsonDriver.importFile(connection, outputTableName, new File(pathFile), new EmptyProgressVisitor())
            outputTableName_full = outputTableName + " & " + outputTableName_full

            ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + outputTableName + "\"")

            int pk2Field = JDBCUtilities.getFieldIndex(rs.getMetaData(), "PK2")
            int pkField = JDBCUtilities.getFieldIndex(rs.getMetaData(), "PK")

            if (pk2Field > 0 && pkField > 0) {
                stmt.execute("ALTER TABLE " + outputTableIdentifier + " DROP COLUMN PK2;")
                logger.warn("The PK2 column automatically created by the SHP driver has been deleted.")
            }

            // Read Geometry Index and type of the table
            List<String> spatialFieldNames = GeometryTableUtilities.getGeometryColumnNames(connection, TableLocation.parse(outputTableName, DBUtils.getDBType(connection)))

            // If the table does not contain a geometry field
            if (spatialFieldNames.isEmpty()) {
                logger.warn("The table " + outputTableName + " does not contain a geometry field.")
            } else {
                stmt.execute('CREATE SPATIAL INDEX IF NOT EXISTS ' + outputTableName + '_INDEX ON ' + outputTableIdentifier + '(the_geom);')
                // Get the SRID of the table
                Integer tableSrid = GeometryTableUtilities.getSRID(connection, outputTableIdentifier);
                println("SRID TABLE : " + tableSrid);
                println("SRID GIVE : " + input["srid"]);

                Integer inputSrid = input.containsKey("srid") ? Integer.parseInt(input["srid"].toString()) : null;

                if (tableSrid != 0 && inputSrid != null && !tableSrid.equals(inputSrid)) {
                    resultString = "The table " + outputTableName + " already has a different SRID than the one you gave.";
                    throw new Exception("ERROR: " + resultString);
                }


                // Replace default SRID by the srid of the table
                if (tableSrid != 0) srid = tableSrid

                // Display the actual SRID in the command window
                logger.info("The SRID of the table " + outputTableName + " is " + srid)

                // If the table does not have an associated SRID, add a SRID
                if (tableSrid == 0) {
                    Statement st = connection.createStatement()
                    GeometryMetaData metaData = GeometryTableUtilities.getMetaData(connection, outputTableIdentifier, spatialFieldNames.get(0));
                    metaData.setSRID(srid);
                    st.execute(String.format("ALTER TABLE %s ALTER COLUMN %s %s USING ST_SetSRID(%s,%d)", outputTableIdentifier, spatialFieldNames.get(0), metaData.getSQL(),spatialFieldNames.get(0) ,srid))
                }
            }

            // If the table has a PK column and doesn't have any Primary Key Constraint, then automatically associate a Primary Key
            ResultSet rs2 = stmt.executeQuery("SELECT * FROM " + outputTableIdentifier)
            int pkUserIndex = JDBCUtilities.getFieldIndex(rs2.getMetaData(), "PK")
            int pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, outputTableIdentifier)

            if (pkIndex == 0) {
                if (pkUserIndex > 0) {
                    stmt.execute("ALTER TABLE " + outputTableIdentifier + " ALTER COLUMN PK INT NOT NULL;")
                    stmt.execute("ALTER TABLE " + outputTableIdentifier + " ADD PRIMARY KEY (PK);  ")
                    resultString = resultString + String.format(outputTableIdentifier.toString() + " has a new primary key constraint on PK")
                    logger.info(String.format(outputTableIdentifier.toString() + " has a new primary key constraint on PK"))
                }
            }

        }
    }

    def queries_conf = """
    ----------------------------------
    -- TEST

    DROP TABLE IF EXISTS nuts_link, metadata;

    """

    StringBuilder stringBuilder = new StringBuilder()
    // print to command window
    def engine = new SimpleTemplateEngine()
    stringBuilder.append(queries_conf)
    def template = engine.createTemplate(stringBuilder.toString()).make(binding)

    def rapport = """
        <!doctype html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width">
            <title data-react-helmet="true">Plamade computation platform</title>
            <link rel="shortcut icon" href="/favicon.ico" type="image/png">
        </head>
        <body>
        <h3>Location $location</h3>
        <hr>
        - Les tables <code>BUILDINGS</code>, <code>ROADS</code>, <code>RAIL_SECTIONS</code>, <code>RAIL_TRAFFIC</code>, <code>SCREENS</code>, 
        <code>LANDCOVER</code>, <code>ZONE</code>, <code>DEM</code> ainsi que celles de configuration ont bien été importées.
        </br></br>
        - Système de projection : EPSG:$srid 
        </br></br>
        - Distance de sélection autour du département et des infrastructures : $buffer m <br/>
        
        </body>
        </html>
    """

    def bindingRapport = ["buffer": buffer, "codeDep": location, "srid" : srid]
    def templateRapport = engine.createTemplate(rapport).make(bindingRapport)

    // print to WPS Builder
    return templateRapport.toString()

}
