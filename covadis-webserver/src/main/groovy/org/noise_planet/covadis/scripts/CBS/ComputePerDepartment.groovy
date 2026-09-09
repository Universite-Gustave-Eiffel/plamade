package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.Field
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.noisemodelling.webserver.database.DatabaseManagement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

title = 'Full NoiseModelling computation that merge all UUEID into a single result for a department'
description = 'Full NoiseModelling computation that merge all UUEID into a single result for a department'

inputs = [
        projectionName: [
                name: "Projection name",
                title: "Projection name",
                description: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ] ,
        department: [
                title: "Department code",
                name: "Department code",
                description: "Department code (insee_dep) eg. 01, 44, 971",
                type: String.class
        ],
        conf: [
                title: "Configuration identifier",
                name: "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type: Integer.class
        ]
]

@Field
static final int batchSize = 100

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {

    Logger logger = LoggerFactory.getLogger(this.class)

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if (!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
        throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
    }

    def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            postgisConfig['user_name'] as String,
            postgisConfig['password'] as String,
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        pgConnection.setAutoCommit(true)
        Sql sql = new Sql(pgConnection)

        def mainConfiguration =
                sql.firstRow("SELECT * FROM cbs_uge_input.nm_conf WHERE confid = ${input.conf}" as String)

        // Fetch nuts table
        logger.info("Fetching NUTS table...")
        Map<String, String> codeDeptToNuts = new HashMap<>()
        sql.rows("SELECT code_dept, code_2021 FROM cbs_uge_input.nm_nuts" as String).each { row ->
            codeDeptToNuts.put(row.code_dept as String, row.code_2021 as String)
        }

        // Log main configuration entries
        logger.info("Configuration:")
        mainConfiguration.each { entry -> logger.info("$entry.key : $entry.value")
        }

        // Fetch all uueid related to this department (on propagation distance from the border of this department)
        List<String> uueids = new ArrayList<>()

        def fetchTableNamesQuery = """
            SELECT distinct uueid FROM cbs_uge_input.nm_link_dept_infra_road_${input.projectionName} nldirh
            WHERE nldirh.insee_dep = '$input.department' ORDER BY uueid;
        """
        Sql pgSql = new Sql(pgConnection)
        pgSql.rows(fetchTableNamesQuery as String).each { row -> uueids.add(row.uueid as String)
        }

        def tempDirectory = File.createTempDir()
        // Create a local H2 database for this task
        DataSource h2DataSource = DatabaseManagement.createH2DataSource(tempDirectory.getAbsolutePath(),
                "h2_${input.department}", "sa", "sa", "", true)
        logger.info("Create database for department ${input.department} in directory: $tempDirectory")
        // Copy the two configuration tables
        try (Connection h2Connection = h2DataSource.getConnection()) {
            try (Statement st = connection.createStatement(); ResultSet rs =
                    st.executeQuery("SELECT * FROM POSTGIS_CONFIGURATION")) {
                PostGISUtilities
                        .copyResultSetToDatabase(connection, rs, h2Connection, "POSTGIS_CONFIGURATION", true, batchSize)
            }
            try (Statement st = connection.createStatement(); ResultSet rs =
                    st.executeQuery("SELECT * FROM SLURM_CONFIGURATION")) {
                PostGISUtilities
                        .copyResultSetToDatabase(connection, rs, h2Connection, "SLURM_CONFIGURATION", true, batchSize)
            }
        }

        computeForDepartment(
                input.department, h2DataSource, pgConnection, progress, input, mainConfiguration, codeDeptToNuts)

        // Delete the database file
        if (h2DataSource instanceof Closeable) {
            ((Closeable) h2DataSource).close()
        }
        new File(tempDirectory, "h2_${input.department}.mv.db").delete()

        // Return results
        return [result: "OK"]
    }
}

def computeForDepartment(department, h2DataSource, pgConnection, progress, input, mainConfiguration, codeDeptToNuts) {

}