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





import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.util.ScriptReader
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.sql.Connection
import java.util.zip.GZIPInputStream

title = 'Load SQL File LDay,Levening,LNight,Lden'
description = ''

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

// main function of the script
def exec(Connection connection, input) {
    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : SQL load file')
    logger.info("inputs {}", input) // log inputs of the run

    // -------------------
    // Get every inputs
    // -------------------

    File scriptFile = new File("Road_Noise_level.sql.gz")
    if("workingDirectory" in input) {
        scriptFile = new File(new File(input["workingDirectory"] as String), "Road_Noise_level.sql.gz")
    }
    if(!scriptFile.exists()) {
        return scriptFile.absolutePath + " does not exists"
    }

    ProgressVisitor progressLogger

    if("progressVisitor" in input) {
        progressLogger = input["progressVisitor"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1);
    }


    parseScript(scriptFile, sql, progressLogger, true)


    resultString = "Calculation Done ! "

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Load SQL File')

    // print to WPS Builder
    return resultString

}
