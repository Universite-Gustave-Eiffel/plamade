package org.noise_planet.covadis.scripts.CBS


import groovy.sql.Sql
import org.apache.commons.codec.binary.Base32
import org.bouncycastle.util.encoders.Base32Encoder
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.noisemodelling.scripts.Import_and_Export.Export_Table
import org.noise_planet.noisemodelling.scripts.Import_and_Export.Import_File
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.nio.ByteBuffer
import java.sql.Connection

title = 'Export PostGIS table to H2GIS database'
name = 'Export PostGIS table to H2GIS database'
description = 'Export PostGIS table to the H2 database using the stored PostGIS settings'

inputs = [
        tableToExport: [
                name:        'PostGIS table to export',
                title:       'PostGIS table to export',
                description: 'Table Name or SQL Query</br> </br>' +
                        '<u>Option 1: Simple table name</u> </br>' +
                        'Enter the name of an existing table, e.g.: <code>mytable</code> </br> </br>' +
                        '<u>Option 2: SQL query with parenthesis</u> </br>' +
                        'Wrap your SELECT query in parenthesis to export filtered or joined data </br>' +
                        'Example: </br><code>(SELECT * FROM mytable WHERE field = 1)</code>',
                type: String.class
        ],
        tableName: [
                name       : 'Output table name',
                title      : 'Name of created table',
                description: 'Name of the table you want to create from the file.',
                type       : String.class
        ],
        intermediateFileFormat: [
                name         : 'Intermediate file format',
                title        : 'Intermediate file format',
                description  : 'This function will first write the content of the table using this file format, for non geometric table you should switch to dbf or json format',
                allowedValues: ["shp", "fgb", "dbf", "json", "geojson"],
                default      : 'fgb',
                type         : String.class
        ],
        ifTableExists: [
                name         : 'Table exists operation',
                title        : 'Table exists operation',
                description  : 'What to do if a table with the same name already exists ?',
                allowedValues: ["Overwrite", "Skip import", "Raise error"],
                default      : 'Overwrite',
                type         : String.class
        ]

]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger("tutorial")

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if(!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
        throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
    }

    def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            postgisConfig['user_name'] as String,
            postgisConfig['password'] as String,
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        ProgressVisitor importProgress = progress.subProcess(2)
        File tempDir = File.createTempDir("nmcbs")
        File tempFile = new File(
                tempDir, "${encodeLongToBase32(System.currentTimeMillis())}.${input.intermediateFileFormat}")
        // Export the PostGIS table to a local file
        new Export_Table().exec(pgConnection, [exportPath: tempFile.absolutePath, tableToExport: input.tableToExport],
                importProgress)
        // Import the file to the H2 database
        def output = new Import_File()
                .exec(connection, [pathFile: tempFile.absolutePath, tableName: input.tableName, ifTableExists:
                        input.ifTableExists], importProgress)
        // remove file
        tempFile.delete()

        // Return results
        return output
    }
}

static def encodeLongToBase32(long number) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(number);
    byte[] bytes = buffer.array();

    Base32 base32 = new Base32();
    return base32.encodeToString(bytes);
}
