package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Field
import org.h2.value.ValueGeometry
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.GeometryMetaData
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.twkb.TWKBWriter
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.noisemodelling.jdbc.utils.DataBaseUtilities
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
        ],
        configuration_name      : [
                name       : 'HPC Configuration Name',
                title      : 'HPC Configuration Name',
                description: 'Slurm SSH access configuration name written through Write_HPC_Settings WPS Script',
                min        : 0, max: 1,
                type       : String.class
        ],
        key_password        : [
                name       : 'SSH Private Key password',
                title      : 'SSH Private Key password',
                description: 'Optional private key password',
                min        : 0, max: 1,
                type       : String.class
        ],
        slurm_task_count            : [
                name       : 'Slurm task count',
                title      : 'Slurm task count',
                description: 'Number of parallel jobs for the computation on the Slurm server.',
                default    : 8,
                type       : Integer.class
        ],
        receiver_rays        : [
                name       : 'Receiver rays profile',
                title      : 'Receiver rays profile',
                description: 'Replace computed receiver by the provided one (WKT) and combine the RAYS table to the main connection',
                min        : 0, max: 1,
                type       : String.class
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
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String)
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        pgConnection.setAutoCommit(true)
        Sql sql = new Sql(pgConnection)

        def mainConfiguration = ComputePerUUEID.fetchNoiseModellingConfiguration(sql, input.conf as Integer)

        // Fetch nuts table
        def codeDeptToNuts = ComputePerUUEID.fetchCodeDeptToNutsMap(sql)

        def tempDirectory = File.createTempDir()
        String databaseName = "h2_${input.department}_" + System.currentTimeSeconds()
        // Create a local H2 database for this task
        DataSource h2DataSource = DatabaseManagement.createH2DataSource(tempDirectory.getAbsolutePath(),
                databaseName, "sa", "sa", "", true)
        logger.info("Create database for department ${input.department} in directory: $tempDirectory")
        try {
            ComputePerUUEID.copyConfigurationTables(h2DataSource, connection)

            computeForDepartment(input.department as String, h2DataSource, pgConnection, progress, input, mainConfiguration, codeDeptToNuts)

            ComputePerUUEID.copyRaysTableToMainConnection(h2DataSource, input, connection)
        } finally {
            // Delete the database file
            if (h2DataSource instanceof Closeable) {
                ((Closeable) h2DataSource).close()
            }
            new File(tempDirectory, "${databaseName}.mv.db").delete()
        }
        // Return results
        return [result: "OK"]
    }
}

/**
 *
 * @param input
 * @param pgConnection
 * @param h2Connection
 * @param stepsProgress
 * @param posSol
 * @param extractionEnvelopeGeometry
 * @return
 */
static def fetchRoads(Map input, Connection pgConnection, Connection h2Connection, String posSol, String extractionEnvelopeGeometry) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch roads sources..")
    def roadsQuery = """SELECT * FROM cbs_uge_output.routier_emission_${input.projectionName} 
      WHERE ST_Intersects(THE_GEOM, '$extractionEnvelopeGeometry'::geometry) AND pos_sol='$posSol' AND (franchisst IS NULL OR franchisst = 'Pont')"""

    try( Statement st = pgConnection.createStatement()
         ResultSet rs = st.executeQuery(roadsQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "LW_SOURCES", true, batchSize)
    }

    // Truncate source by the department + maximum propagation distance
    truncateSources(h2Connection, extractionEnvelopeGeometry)
}

/**
 *
 * @param h2Connection
 * @param extractionEnvelopeGeometry
 * @return
 */
static truncateSources(Connection h2Connection, String extractionEnvelopeGeometry) {
    ComputePerUUEID.runSqlQuery(h2Connection, """
        -- Truncate sources by the department + maximum propagation distance
        UPDATE LW_SOURCES SET THE_GEOM = ST_Intersection(THE_GEOM, '$extractionEnvelopeGeometry'::geometry);
        DELETE FROM LW_SOURCES WHERE ST_ISEMPTY(THE_GEOM);
        """ as String)
}

/**
 *
 * @param input
 * @param pgConnection
 * @param h2Connection
 * @param stepsProgress
 * @param posSol
 * @param extractionEnvelopeGeometry
 * @return
 */
static def fetchRails(Map input, Connection pgConnection, Connection h2Connection, String posSol, String extractionEnvelopeGeometry) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch rails sources..")
    def roadsQuery = """SELECT fe.* FROM cbs_uge_output.ferroviaire_emission_${input.projectionName}  fe
    INNER JOIN cbs_uge_output.ferroviaire_troncon_${input.projectionName} ft ON (fe.pk_section = ft.pk_section) 
      WHERE ST_Intersects(fe.THE_GEOM, '$extractionEnvelopeGeometry'::geometry) AND pos_sol='$posSol'"""

    try( Statement st = pgConnection.createStatement()
         ResultSet rs = st.executeQuery(roadsQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "LW_SOURCES", true, batchSize)
    }

    // Truncate source by the department + maximum propagation distance
    truncateSources(h2Connection, extractionEnvelopeGeometry)
}

/**
 *
 * @param department
 * @param h2DataSource
 * @param pgConnection
 * @param progress
 * @param input
 * @param mainConfiguration
 * @param codeDeptToNuts
 */
def computeForDepartment(String department, DataSource h2DataSource, Connection pgConnection, ProgressVisitor progress, Map input, Map mainConfiguration, Map<String, String> codeDeptToNuts) {
    def pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Computing for department: $department")
    ProgressVisitor departmentProgress = progress.subProcess(2)
    String extractionEnvelopeGeometryWKT

    try (Connection h2Connection = h2DataSource.getConnection()) {
        // Compute envelope of the simulation
        def projectionCode = Generate_roads_sources.getSRIDFromTableExtensionName()[input.projectionName as String]
        def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(the_geom, ${
            mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_input.nm_departement_$projectionCode WHERE insee_dep = '${department}';""" as String)
        if (res == null) {
            throw new IllegalArgumentException("No match for the provided department '${department}'")
        }

        def extractionEnvelopeGeometry = res.geomenv as Geometry
        // Switch the geometry factory in order to use the SRID of the geometry into the geometry factory
        GeometryFactory geometryFactory = new GeometryFactory(extractionEnvelopeGeometry.getFactory().getPrecisionModel(), extractionEnvelopeGeometry.getSRID())
        extractionEnvelopeGeometry = geometryFactory.createGeometry(extractionEnvelopeGeometry)
        extractionEnvelopeGeometryWKT = ValueGeometry.getFromGeometry(extractionEnvelopeGeometry).string

        ComputePerUUEID.fetchDem(input, extractionEnvelopeGeometryWKT, h2Connection, pgConnection, departmentProgress)

        ComputePerUUEID.fetchBuildings(input,
                pgConnection,
                extractionEnvelopeGeometryWKT,
                h2Connection, new EmptyProgressVisitor(), mainConfiguration.wall_alpha as Double)

        ComputePerUUEID.generateReceivers(extractionEnvelopeGeometry, h2Connection,
                mainConfiguration.confdistbuildingsreceivers as Double, mainConfiguration, new EmptyProgressVisitor(),
                ComputePerUUEID.distanceFacadeBuildingsReceivers,
                ComputePerUUEID.distanceDelaunayBufferFacade, pgConnection, input.projectionName as String)

        keepOnlyReceiversInDepartment(input, pgConnection, h2Connection)

        ComputePerUUEID.processLandCover(input, pgConnection, extractionEnvelopeGeometryWKT, h2Connection)

        // Look for the station near the centroid of the department
        ComputePerUUEID.fetchAtmosphericPeriodFromStations(input, pgConnection, extractionEnvelopeGeometry.centroid, h2Connection, new EmptyProgressVisitor())
    }
    // Separate propagation for roads and rails but common input data
    ProgressVisitor sourceProgress = departmentProgress.subProcess(2)
    for (def source : ["road", "rail"]) {
            def posSols ;

        if(source == "road") {
            posSols = pgSql.rows("""SELECT DISTINCT pos_sol FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg
            WHERE ST_Intersects(THE_GEOM, '$extractionEnvelopeGeometryWKT'::geometry) AND (franchisst IS NULL OR franchisst = 'Pont')""" as String)
                    .collect {
                        it.pos_sol as String
                    }
        } else {
            posSols = pgSql.rows("""SELECT DISTINCT pos_sol FROM
                        cbs_uge_output.ferroviaire_troncon_${input.projectionName}  WHERE ST_Intersects(THE_GEOM, '$extractionEnvelopeGeometryWKT'::geometry)""" as String)
                    .collect {
                        it.pos_sol as String
                    }
        }

        ProgressVisitor solProgress = sourceProgress.subProcess(posSols.size())
        new ArrayList<>(posSols).forEach { String posSol ->
            logger.info("Compute for pos_sol = $posSol for $source sources")
            boolean doCompute
            try (Connection h2Connection = h2DataSource.getConnection()) {
                if(source == "road") {
                    fetchRoads(input, pgConnection, h2Connection, posSol, extractionEnvelopeGeometryWKT)
                } else {
                    fetchRails(input, pgConnection, h2Connection, posSol, extractionEnvelopeGeometryWKT)
                }

                if (input.containsKey("receiver_rays") && (input.receiver_rays as String).length() > 0) {
                    doCompute = true
                    ComputePerUUEID.generateDebugReceiver(h2Connection, input)
                } else {
                    doCompute = ComputePerUUEID.GenerateReceiversFiltered(h2Connection, logger, posSol,
                            mainConfiguration.confmaxsrcdist * 1.2d as Double,
                            ComputePerUUEID.distanceFacadeBuildingsReceivers + 0.2)
                    // add an epsilon to guaranty ignoring the closest wall
                }
            }

            if (doCompute) {
                try (Connection h2Connection = h2DataSource.getConnection()) {
                    // Adapt the DEM with this special road/train infrastructure height platforms
                    if(source == "road") {
                        ComputePerUUEID.enrichDemWithRoads(input, h2Connection, pgConnection, new EmptyProgressVisitor(), posSol, "ST_Intersects(geom, '$extractionEnvelopeGeometryWKT'::geometry)")
                    } else {
                        ComputePerUUEID.enrichDemWithRails(input, h2Connection, pgConnection, posSol, "ST_Intersects(the_geom, '$extractionEnvelopeGeometryWKT'::geometry)")
                    }
                }
                // Run the simulation with this road height
                ComputePerUUEID.runSimulation(input, mainConfiguration, h2DataSource, posSol, solProgress)
                ComputePerUUEID.mergeOutputRaysTable(h2DataSource, input, posSol)
            } else {
                logger.info("Skip pos_sol {}", posSol)
                posSols.removeElement(posSol)
                solProgress.endStep()
            }
        }


        try (Connection h2Connection = h2DataSource.getConnection()) {
            // Merge noise levels for each pos sols
            ComputePerUUEID.mergeReceiversLevels(posSols, h2Connection)

            if (!(input.containsKey("receiver_rays") && (input.receiver_rays as String).length() > 0)) {
                // Generate IsoContours
                // Convert 78 to 078 and A71 to A71
                def nutsCode = ""
                def codeDept = department.length() < 3 ? department.padLeft(3, '0') : department
                try {
                    nutsCode = codeDeptToNuts.get(codeDept)
                } catch (NumberFormatException e) {
                    logger.error("Invalid department code: {}", department)
                }
                ComputePerUUEID.generateCBS(h2Connection, new EmptyProgressVisitor(), source == "road" ? NoiseSource.ROAD : NoiseSource.HIGH_SPEED_TRAIN)

                cutCbsByDepartmentPolygon(input, pgConnection, h2Connection)

                ComputePerUUEID.generateBuildingsFacadeExpo(h2Connection)

                def cpi = pgSql.firstRow("SELECT (taux_incidence / 100000) cpi_per_person FROM cbs_uge_input.cpi WHERE codedept = $codeDept").cpi_per_person as Double
                logger.info("CPI per person for department {}: {}", department, cpi)
                ComputePerUUEID.generateExposureStatisticsFromFacadeExpo(h2Connection, cpi)

                // Upload indicators
                uploadIndicatorsTables(h2Connection, pgConnection, department, nutsCode, input.projectionName as String,  source == "road" ? 'R' : 'F')

                // Upload CBS Table to remote PostGIS database
                uploadCBS(h2Connection, pgConnection, department, input.projectionName as String,  source == "road" ? 'R' : 'F')
            }
        }
    }
}

/**
 * Upload the content of the facade exposure table to PostGIS
 * @param h2Connection Local h2 connection
 * @param pgConnection Remote PostGIS connection
 * @param uueid Infrastructure identifier
 * @param projectionName Projection name ex: hexa
 */
static def uploadIndicatorsTables(Connection h2Connection, Connection pgConnection, String department, String nutsCode, String projectionName, String typeSource) {
    Logger logger = LoggerFactory.getLogger(ComputePerDepartment.class)
    boolean tableExists = JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.facade_expo_dept_$projectionName")
    if(tableExists) {
        DataBaseUtilities.parseScript("""
            DELETE FROM cbs_uge_output.facade_expo_dept_$projectionName WHERE insee_dep = '$department' and typesource = '$typeSource';
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }

    try( Statement st = h2Connection.createStatement()
         ResultSet rs = st.executeQuery("""SELECT '$department' insee_dep,'$typeSource' as typesource, the_geom, idbat, lden, ln FROM FACADE_EXPO""")) {
        PostGISUtilities.copyResultSetToDatabase(h2Connection, rs, pgConnection,
                "cbs_uge_output.facade_expo_dept_$projectionName", false, batchSize)
    }

    if(!tableExists) {
        // Create index
        DataBaseUtilities.parseScript("""            
            CREATE INDEX ON cbs_uge_output.facade_expo_dept_$projectionName USING GIST (the_geom);
            CREATE INDEX ON cbs_uge_output.facade_expo_dept_$projectionName (insee_dep);
            CREATE INDEX ON cbs_uge_output.facade_expo_dept_$projectionName (idbat);
            ALTER TABLE cbs_uge_output.facade_expo_dept_$projectionName OWNER TO cbs_uge_group;
            COMMENT ON TABLE cbs_uge_output.facade_expo_dept_$projectionName IS 'Contains the noise levels on receivers placed on the facade of buildings for each department.';
        """, pgConnection, new EmptyProgressVisitor(), logger)

    }

    tableExists = JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.expo_dept")
    if(tableExists) {
        DataBaseUtilities.parseScript("""
            DELETE FROM cbs_uge_output.expo_dept WHERE insee_dep = '$department' and typesource = '$typeSource';
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }
    try( Statement st = h2Connection.createStatement()
         ResultSet rs = st.executeQuery("""SELECT CONCAT('${department}', '_${typeSource}_', NOISELEVEL) pk,'${typeSource}' as typesource, '${nutsCode}' nutscode ,
                 '$department' insee_dep, NOISELEVEL, ROUND(PEOPLE)::integer PEOPLE,
                 ROUND(DWELLINGS)::integer DWELLINGS, HOSPITALS , SCHOOLS , HA  , HSD , AREA , INDICETYPE 
                 FROM EXPO""")) {
        PostGISUtilities.copyResultSetToDatabase(h2Connection, rs, pgConnection,
                "cbs_uge_output.expo_dept", false, batchSize)
    }
    if(!tableExists) {
        // Create index
        DataBaseUtilities.parseScript("""
            CREATE INDEX ON cbs_uge_output.expo_dept(insee_dep);       
            ALTER TABLE cbs_uge_output.expo_dept ALTER COLUMN pk SET NOT NULL;
            ALTER TABLE cbs_uge_output.expo_dept ADD PRIMARY KEY (pk);
            ALTER TABLE cbs_uge_output.expo_dept OWNER TO cbs_uge_group;
            COMMENT ON TABLE cbs_uge_output.expo_dept IS 'Assessment of health risks (HA, HSD) associated with exposure to transportation noise for each noise levels';
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }


    tableExists = JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.expo_global_dept")
    if(tableExists) {
        DataBaseUtilities.parseScript("""
            DELETE FROM cbs_uge_output.expo_global_dept WHERE insee_dep = '$department';
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }
    try( Statement st = h2Connection.createStatement()
         ResultSet rs = st.executeQuery("""SELECT '$department' insee_dep, '$nutsCode' as NUTSCODE ,'${typeSource}' as typesource, CPI, HA, HSD, TOTAL_POP FROM EXPO_GLOBAL""")) {
        PostGISUtilities.copyResultSetToDatabase(h2Connection, rs, pgConnection,
                "cbs_uge_output.expo_global_dept", false, batchSize)
    }
    if(!tableExists) {
        // Create index, set insee_dep as primary key
        DataBaseUtilities.parseScript("""
            COMMENT ON TABLE cbs_uge_output.expo_global_dept IS 'Global exposure table for each department';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.insee_dep IS 'Department Code';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.nutscode IS 'NUTS Code';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.cpi IS 'Nombre de personnes affectées par les cardiopathies ischémiques';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.ha IS 'Nombre de personnes affectées par la forte gêne';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.hsd IS 'Nombre de personnes affectées par les fortes perturbations du sommeil';
            COMMENT ON COLUMN cbs_uge_output.expo_global_dept.total_pop IS 'Population totale pour le calcul des indicateurs';
            ALTER TABLE cbs_uge_output.expo_global_dept ALTER COLUMN insee_dep SET NOT NULL;
            ALTER TABLE cbs_uge_output.expo_global_dept ADD PRIMARY KEY (insee_dep);            
            ALTER TABLE cbs_uge_output.expo_global_dept OWNER TO cbs_uge_group;
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }
}

@CompileStatic
static def uploadCBS(Connection h2Connection, Connection pgConnection, String department, String projectionName, String typeSource) {
    Sql pgSql = new Sql(pgConnection)
    Sql h2Sql = new Sql(h2Connection)
    int batchSize = 100
    Logger logger = LoggerFactory.getLogger(ComputePerDepartment.class)

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, "ISOPHONES", "THE_GEOM")

    if (!JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.CBS_DEPT_$projectionName")) {
        DataBaseUtilities.parseScript("""
            CREATE TABLE cbs_uge_output.CBS_DEPT_$projectionName (
                pk varchar not null primary key,
                the_geom ${metaData.getSQL()},
                insee_dep varchar not null,
                cbstype varchar,
                typesource varchar,
                indicetype varchar,
                noiselevel varchar);
            CREATE INDEX ON cbs_uge_output.CBS_DEPT_$projectionName USING GIST (the_geom);
            CREATE INDEX ON cbs_uge_output.CBS_DEPT_$projectionName (insee_dep);
            ALTER TABLE cbs_uge_output.CBS_DEPT_$projectionName OWNER TO cbs_uge_group;
        """, pgConnection, new EmptyProgressVisitor(), logger)
    } else {
        DataBaseUtilities.parseScript("""
            -- Remove previously generated CBS from the cbs_uge_output.CBS_DEPT_$projectionName table
            DELETE FROM cbs_uge_output.CBS_DEPT_$projectionName WHERE insee_dep = '$department' and typesource = '$typeSource';
        """, pgConnection, new EmptyProgressVisitor(), logger)
    }


    def insertSql = """
        INSERT INTO cbs_uge_output.CBS_DEPT_$projectionName
        (pk, the_geom, insee_dep, cbstype, typesource, indicetype, noiselevel) 
        VALUES (?, ST_GeomFromTWKB(?), ?, ?, ?, ?, ?)
    """ as String

    TWKBWriter twkbWriter = new TWKBWriter()
    twkbWriter.setEncodeZ(true)
    twkbWriter.setXYPrecision(2)
    twkbWriter.setZPrecision(2)
    // Upload only CBS type A, because producing type C would require to split noise sources by train "typeligne"
    pgSql.withBatch(batchSize, insertSql) {
        h2Sql.eachRow("SELECT the_geom, cbstype, typesource, PERIOD, noiselevel FROM ISOPHONES where cbstype = 'A'" as String) { row ->
            // Insert into PostGIS table
            it.addBatch(
                    department+ '_' + row.getString("noiselevel")+"_"+row.getString("typesource"),
                    twkbWriter.write(row.getObject("the_geom") as Geometry),
                    department,
                    row.getString("cbstype"),
                    row.getString("typesource"),
                    row.getString("PERIOD"),
                    row.getString("noiselevel"))
        }
    }


}

static keepOnlyReceiversInDepartment(Map input,Connection pgConnection, Connection h2Connection) {
    def pgSql = new Sql(pgConnection)
    def h2Sql = new Sql(h2Connection)
    def projectionCode = Generate_roads_sources.getSRIDFromTableExtensionName()[input.projectionName as String]
    def department = input.department as String
    def extractionEnvelopeGeometry = getDepartmentGeometry(pgSql, projectionCode, department)
    // Remove from the table all triangles that does not touch the department geometry
    int removed = h2Sql.executeUpdate("""DELETE FROM TRIANGLES T WHERE NOT ST_Intersects(THE_GEOM, $extractionEnvelopeGeometry)""")
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Removed {} triangles that do not intersect with the department geometry", removed)
    // Remove receivers not referenced by triangles and remove buildings facade receivers not in department
    ComputePerUUEID.runSqlQuery(h2Connection,"""
        DELETE FROM RECEIVERS_DELAUNAY R 
            WHERE NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_1 = R.PK)
              AND NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_2 = R.PK)
              AND NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_3 = R.PK);
        SET @LASTDELAUNAY=(SELECT last_delaunay FROM RECEIVERS_OFFSET);
        -- Remove receivers that does not exists anymore on RECEIVERS table
        DELETE FROM RECEIVERS WHERE PK <= @LASTDELAUNAY AND PK NOT IN (SELECT PK FROM RECEIVERS_DELAUNAY);
        """ as String)
    // Remove receivers PK > @LASTDELAUNAY were the building centroid are outside of the department
    removed = h2Sql.executeUpdate("""
        DELETE FROM RECEIVERS WHERE PK > @LASTDELAUNAY AND (PK - @LASTDELAUNAY) NOT IN (SELECT RB.PK FROM RECEIVERS_BUILDINGS RB INNER JOIN BUILDINGS B ON B.PK = RB.build_pk WHERE ST_Intersects(ST_Centroid(B.THE_GEOM), $extractionEnvelopeGeometry))
    """)
    logger.info("Removed {} building facade receivers that are outside of the department geometry", removed)
}

static Geometry getDepartmentGeometry(Sql pgSql, int projectionCode, department) {
    def res = pgSql.firstRow("""SELECT 
             st_simplify(the_geom, 1) geomenv
             FROM cbs_uge_input.nm_departement_$projectionCode WHERE insee_dep = '${department}';""" as String)
    if (res == null) {
        throw new IllegalArgumentException("No match for the provided department '${department}'")
    }
    res.geomenv as Geometry
}

static void cutCbsByDepartmentPolygon(Map input,Connection pgConnection, Connection h2Connection) {
    def h2Sql = new Sql(h2Connection)
    def projectionCode = Generate_roads_sources.getSRIDFromTableExtensionName()[input.projectionName as String]
    def department = input.department as String
    def extractionEnvelopeGeometry = getDepartmentGeometry(new Sql(pgConnection), projectionCode, department)
    h2Sql.executeUpdate("""
        UPDATE ISOPHONES SET THE_GEOM = ST_Multi(ST_Intersection(THE_GEOM, $extractionEnvelopeGeometry))
    """)
    // Remove empty geometries
    h2Sql.executeUpdate("""
        DELETE FROM ISOPHONES WHERE ST_ISEMPTY(THE_GEOM);
    """)
}