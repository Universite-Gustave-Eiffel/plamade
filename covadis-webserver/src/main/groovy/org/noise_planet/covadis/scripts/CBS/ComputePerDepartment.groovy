package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.Field
import org.h2.value.ValueGeometry
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Geometry
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities
import org.noise_planet.noisemodelling.scripts.Database_Manager.Add_Primary_Key
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

        def mainConfiguration = ComputePerUUEID.fetchNoiseModellingConfiguration(sql, input.conf as Integer)

        // Fetch nuts table
        def codeDeptToNuts = ComputePerUUEID.fetchCodeDeptToNutsMap(sql)


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
        ComputePerUUEID.copyConfigurationTables(h2DataSource, connection)

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

def computeForDepartment(String department, h2DataSource, Connection pgConnection, ProgressVisitor progress, Map input, mainConfiguration, Map codeDeptToNuts) {
    def pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Computing for department: $department")
    try (Connection h2Connection = h2DataSource.getConnection()) {
        // Compute envelope of the simulation
        def projectionCode = Generate_sources.getSRIDFromTableExtensionName()[input.projectionName as String]
        def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(the_geom, ${
            mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_input.nm_departement_$projectionCode WHERE insee_dep = '${department}';""" as String)
        if (res == null) {
            throw new IllegalArgumentException("No match for the provided department '${department}'")
        }

        def extractionEnvelopeGeometry = res.geomenv as Geometry
        def extractionEnvelopeGeometryWKT = ValueGeometry.getFromGeometry(extractionEnvelopeGeometry).string

        fetchDem(input, extractionEnvelopeGeometryWKT, h2Connection, pgConnection, progress)

        ComputePerUUEID.fetchBuildings(input,
                pgConnection,
                extractionEnvelopeGeometryWKT,
                h2Connection, new EmptyProgressVisitor(), mainConfiguration.wall_alpha as Double)

        fetchAllRoadsUsingInseeDep(input, pgConnection, h2Connection)

        ComputePerUUEID.generateReceivers(extractionEnvelopeGeometry, h2Connection,
                mainConfiguration.confdistbuildingsreceivers as Double, mainConfiguration, new EmptyProgressVisitor())
    }
}


def fetchDem(Map input, String extractionEnvelopeGeometry, Connection h2Connection, Connection pgConnection, ProgressVisitor stepsProgress) {
    Sql pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch digital elevation model..")
    def fetchTableNamesQuery = """
        SELECT bd_alti
        FROM cbs_uge_input.nm_link_dept_infra_road_${input.projectionName} nldirh
        WHERE nldirh.insee_dep = '${input.department}';
    """
    def bdAltiTableName = new HashSet<String>()
    pgSql.rows(fetchTableNamesQuery as String).each { row ->
        bdAltiTableName.add(row.bd_alti as String)
    }

    ComputePerUUEID.fetchDemFromTableList(stepsProgress, bdAltiTableName, pgConnection, h2Connection, extractionEnvelopeGeometry, input)
}


static void fetchAllRoadsUsingInseeDep(Map input, Connection pgConnection, Connection h2Connection) {
    // Fetch all roads using the UUEID query
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName} INNER JOIN cbs_uge_input.nm_link_dept_infra_road_${input.projectionName}
        ON cbs_uge_input.n_routier_troncon_l_${input.projectionName}.uueid = cbs_uge_input.nm_link_dept_infra_road_${input.projectionName}.uueid
        WHERE cbs_uge_input.nm_link_dept_infra_road_${input.projectionName}.insee_dep = '${input.department}'"""
    try (Statement st = pgConnection.createStatement();
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
    }

    ScriptUtilities.execScript(new Add_Primary_Key(), h2Connection, [tableName: "ROADS", pkName: "PK"], new EmptyProgressVisitor())
}