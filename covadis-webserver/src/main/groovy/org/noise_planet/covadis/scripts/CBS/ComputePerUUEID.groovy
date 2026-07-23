package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Field
import org.h2.value.ValueGeometry
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.GeometryMetaData
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.twkb.TWKBWriter
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities
import org.noise_planet.noisemodelling.scripts.Acoustic_Tools.Create_Isosurface
import org.noise_planet.noisemodelling.scripts.Database_Manager.Add_Primary_Key
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query
import org.noise_planet.noisemodelling.scripts.Geometric_Tools.Enrich_DEM_with_road
import org.noise_planet.noisemodelling.scripts.NoiseModelling.Noise_level_from_source
import org.noise_planet.noisemodelling.scripts.Receivers.Building_Grid
import org.noise_planet.noisemodelling.scripts.Receivers.Delaunay_Grid
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.h2gis.utilities.GeometryTableUtilities

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

title = 'Full NoiseModelling computation that output results for each UUEID separately'
description = 'Full NoiseModelling computation that output results for each UUEID separately'

inputs = [
        projectionName: [
                name         : "Projection name",
                title        : "Projection name",
                description  : "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type         : String.class
        ],
        uueid_pattern : [
                title      : "UUEID pattern",
                name       : "UUEID pattern",
                description: "UUEID pattern on roads to extract. eg. RD_FR_00_044% <p>A percent sign % - represents zero, one, or multiple characters</p>" +
                        "<p>A underscore sign _ - represents a single character</p>",
                type       : String.class
        ],
        conf          : [
                title      : "Configuration identifier",
                name       : "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type       : Integer.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

@Field
int batchSize = 100

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

        def mainConfiguration = sql.firstRow("SELECT * FROM cbs_uge_input.nm_conf WHERE confid = ${input.conf}" as String)

        // Fetch nuts table
        logger.info("Fetching NUTS table...")
        Map<String, String> codeDeptToNuts = new HashMap<>()
        sql.rows("SELECT code_dept, code_2021 FROM cbs_uge_input.nm_nuts" as String).each {
            row ->
                codeDeptToNuts.put(row.code_dept as String, row.code_2021 as String)
        }

        // Log main configuration entries
        logger.info("Configuration:")
        mainConfiguration.each {   entry ->
            logger.info("$entry.key : $entry.value")
        }

        List<String> uueids = new ArrayList<>()
        sql.rows("SELECT DISTINCT uueid from cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid LIKE '${input.uueid_pattern}'" as String).each {
            row ->
                uueids.add(row.uueid as String)
        }
        ProgressVisitor stepsProgress = progress.subProcess(uueids.size()) // long running sub tasks

        if (uueids.isEmpty()) {
            throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
        }

        uueids.each {
            computeForUUEID(it, connection, pgConnection, stepsProgress, input, mainConfiguration, codeDeptToNuts)
        }

        // Return results
        return [result: "OK"]
    }


}

def computeForUUEID(String uueid, Connection h2Connection, Connection pgConnection, ProgressVisitor progress, Map input, Map mainConfiguration, Map<String, String> codeDeptToNuts) {
    ProgressVisitor stepsProgress = progress.subProcess(3) // long running sub tasks
    def pgSql = new Sql(pgConnection)
    def h2Sql = new Sql(h2Connection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Computing for UUEID: $uueid")

    // Compute envelope of the simulation
    def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(st_convexhull(st_collect(the_geom)), ${mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE uueid LIKE '$uueid';""" as String)
    if (res == null) {
        throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
    }

    def extractionEnvelopeGeometry = res.geomenv as Geometry
    def extractionEnvelopeGeometryWKT = ValueGeometry.getFromGeometry(extractionEnvelopeGeometry).string

    logger.info("SRID: {}", (extractionEnvelopeGeometry).getSRID())

    processBuildings(input, pgConnection, extractionEnvelopeGeometryWKT, h2Connection, stepsProgress, mainConfiguration.wall_alpha as Double)

    generateReceivers(input, pgConnection, uueid, extractionEnvelopeGeometry, h2Connection,
            mainConfiguration.confdistbuildingsreceivers as Double, mainConfiguration, stepsProgress)

    processLandCover(input, pgConnection, extractionEnvelopeGeometryWKT, h2Connection, stepsProgress)

    fetchAtmosphericPeriodFromStations(input, pgConnection, uueid, h2Connection, stepsProgress)

    def posSolQuery = """SELECT DISTINCT pos_sol FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE (franchisst IS NULL OR franchisst = 'Pont')""" as String

    def posSols = pgSql.rows(posSolQuery).collect { it.pos_sol as String}

    ProgressVisitor solProgress = stepsProgress.subProcess(posSols.size())
    posSols.forEach {posSol ->
        logger.info("Compute for pos_sol = $posSol")
        // Clear tables
        h2Sql.execute("DROP TABLE IF EXISTS DEM, LW_ROADS")
        // Fetch specific road emission at this special height
        processRoads(input, pgConnection, uueid, h2Connection, solProgress, posSol)
        // Adapt the DEM with this special road height
        fetchDem(input, uueid, extractionEnvelopeGeometryWKT, h2Connection, pgConnection, solProgress, posSol)
        // Run the simulation with this road height
        runSimulation(mainConfiguration, h2Connection, posSol, solProgress)
    }

    // Merge noise levels for each pos sols
    mergeReceiversLevels(posSols, h2Connection, uueid, logger, h2Sql)

    // Generate IsoContours
    generateRoadsCBS(h2Connection, uueid, stepsProgress, codeDeptToNuts)

    // Upload CBS Table to remote PostGIS database
    uploadCBS(h2Connection, pgConnection, uueid, input.projectionName as String)

}

@CompileStatic
static def uploadCBS(Connection h2Connection, Connection pgConnection, String uueid, String projectionName) {
    Sql pgSql = new Sql(pgConnection)
    Sql h2Sql = new Sql(h2Connection)
    int batchSize = 100

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, "ISOPHONES", "THE_GEOM");

    if (!JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.CBS_$projectionName")) {
        new Execute_Query().exec(pgConnection, [sqlQueries: """
            CREATE TABLE cbs_uge_output.CBS_$projectionName (
                the_geom ${metaData.getSQL()},
                cbstype varchar,
                typesource varchar,
                indicetype varchar,
                nutscode varchar,
                pk varchar not null,
                uueid varchar,
                noiselevel varchar);
            ALTER TABLE cbs_uge_output.CBS_$projectionName ADD CONSTRAINT cbs_pk_$projectionName PRIMARY KEY (pk);
            ALTER TABLE cbs_uge_output.CBS_$projectionName OWNER TO cbs_uge_group;
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())
    }

    new Execute_Query().exec(pgConnection, [sqlQueries: """
            DELETE FROM cbs_uge_output.CBS_$projectionName WHERE uueid = '$uueid';
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    def insertSql = """
        INSERT INTO cbs_uge_output.CBS_$projectionName
        (the_geom, cbstype, typesource, indicetype, nutscode, pk, uueid, noiselevel) 
        VALUES (ST_GeomFromTWKB(?), ?, ?, ?, ?, ?, ?, ?)
    """ as String

    TWKBWriter twkbWriter = new TWKBWriter()
    twkbWriter.setEncodeZ(true)
    twkbWriter.setXYPrecision(2)
    twkbWriter.setZPrecision(2)
    pgSql.withBatch(batchSize, insertSql) {
        h2Sql.eachRow("SELECT the_geom, cbstype, typesource, PERIOD, nutscode, pk, uueid, noiselevel FROM ISOPHONES") { row ->
            // Insert into PostGIS table
            it.addBatch(twkbWriter.write(row.getObject("the_geom") as Geometry),
                    row.getString("cbstype"), row.getString("typesource"),
                    row.getString("PERIOD"), row.getString("nutscode"),
                    row.getString("pk"), row.getString("uueid"),
                    row.getString("noiselevel"))
        }
    }


}


/**
 * <p>Precondition: The RECEIVERS_LEVEL_$uueid table must exist when calling this function.</p>
 * @param h2Connection Connection to h2 database
 * @param uueid Infrastructure identifier
 * @param progress Progress feedback instance
 * @param codeDeptToNuts Mapping of department codes to NUTS codes
 */
def generateRoadsCBS(Connection h2Connection, String uueid, ProgressVisitor progress, Map<String, String> codeDeptToNuts) {
    Logger logger = LoggerFactory.getLogger(this.class)
    ProgressVisitor stepsProgress = progress.subProcess(2)

    // 1. Extract metadata
    def codeDept = uueid.split("_")[3].substring(0, 3)
    def nutsCode = codeDeptToNuts.get(codeDept)
    logger.info("Processing CBS uueid: $uueid, codeDept: $codeDept, nutsCode: $nutsCode")

    // 2. Prepare Noise Level Tables
    setupReceiverTables(h2Connection, uueid)

    // 3. Define the Noise Level CASE statements for CBS Type A
    def caseLdenA = "(CASE WHEN ISOLABEL = '55-60' THEN 'Lden5559' WHEN ISOLABEL = '60-65' THEN 'Lden6064' WHEN ISOLABEL = '65-70' THEN 'Lden6569' WHEN ISOLABEL = '70-75' THEN 'Lden7074' WHEN ISOLABEL = '75+' THEN 'LdenGreaterThan75' END)"
    def caseLnightA = "(CASE WHEN ISOLABEL = '50-55' THEN 'Lnight5054' WHEN ISOLABEL = '55-60' THEN 'Lnight5559' WHEN ISOLABEL = '60-65' THEN 'Lnight6064' WHEN ISOLABEL = '65-70' THEN 'Lnight6569' WHEN ISOLABEL = '70+' THEN 'LnightGreaterThan70' END)"

    // 4. Generate the 4 CBS Maps

    // CBS A - Day/Evening/Night
    processMap(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_DEN_$uueid", "55.0,60.0,65.0,70.0,75.0,200.0", caseLdenA, "LD", "A", "ISOLVL > 0")

    // CBS A - Night
    processMap(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_NIGHT_$uueid", "50.0,55.0,60.0,65.0,70.0,200.0", caseLnightA, "LN", "A", "ISOLVL > 0")

    // CBS C - Day/Evening/Night
    processMap(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_DEN_$uueid", "68.0,200.0", "'LdenGreaterThan68'", "LD", "C", "ISOLVL = 1")

    // CBS C - Night
    processMap(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_NIGHT_$uueid", "62.0,200.0", "'LdenGreaterThan62'", "LN", "C", "ISOLVL = 1")
}

/**
 * <p>This function creates temporary tables for DEN (Day/Evening/Night) and N (Night) periods
 * by splitting the data from RECEIVERS_LEVEL_$uueid into separate tables with primary keys.</p>
 *
 * @param h2Connection The database connection
 * @param uueid The UUEID identifier used to qualify table names
 */
private void setupReceiverTables(Connection h2Connection, String uueid) {
    def sql = """
        DROP TABLE IF EXISTS RECEIVERS_LEVEL_DEN_$uueid, RECEIVERS_LEVEL_NIGHT_$uueid;
        CREATE TABLE RECEIVERS_LEVEL_DEN_$uueid AS SELECT THE_GEOM, IDRECEIVER, LAEQ FROM RECEIVERS_LEVEL_$uueid WHERE PERIOD='DEN';
        ALTER TABLE RECEIVERS_LEVEL_DEN_$uueid ALTER COLUMN IDRECEIVER INTEGER NOT NULL;
        ALTER TABLE RECEIVERS_LEVEL_DEN_$uueid ADD PRIMARY KEY (IDRECEIVER);
        CREATE TABLE RECEIVERS_LEVEL_NIGHT_$uueid AS SELECT THE_GEOM, IDRECEIVER, LAEQ FROM RECEIVERS_LEVEL_$uueid WHERE PERIOD='N';
        ALTER TABLE RECEIVERS_LEVEL_NIGHT_$uueid ALTER COLUMN IDRECEIVER INTEGER NOT NULL;
        ALTER TABLE RECEIVERS_LEVEL_NIGHT_$uueid ADD PRIMARY KEY (IDRECEIVER);
    """
    new Execute_Query().exec(h2Connection, [sqlQueries: sql, outputFormat: "json"], new EmptyProgressVisitor())
}

/**
 * Main sub-function to process Isosurfaces and Insert into ISOPHONES
 */
private void processMap(Connection conn, ProgressVisitor progress, String uueid, String nutsCode, String sourceTable, String isoClass, String noiseLevelExpr, String period, String cbsType, String filter) {

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(conn, sourceTable, "THE_GEOM");
    // 1. Initialize ISOPHONES table if not exists
    new Execute_Query().exec(conn, [sqlQueries: """CREATE TABLE IF NOT EXISTS ISOPHONES
                (the_geom GEOMETRY(MULTIPOLYGONZ, ${metaData.getSRID()}), pk varchar not null , UUEID varchar,
                 PERIOD varchar, NOISELEVEL varchar, AREA float, cbstype varchar, nutscode varchar, typesource varchar);
                """ as String, outputFormat: "json"], new EmptyProgressVisitor())


    // 2. Execute Isosurface creation
    ScriptUtilities.execScript(new Create_Isosurface(), conn, [
            resultTable: sourceTable,
            smoothCoefficient: 0,
            isoClass: isoClass
    ], progress)

    // 3. Insert results into ISOPHONES
    def insertSql = """
        INSERT INTO ISOPHONES(the_geom, pk, area, uueid, period, noiselevel, cbstype, nutscode, typesource) 
        SELECT ST_Accum(THE_GEOM) THE_GEOM, concat('$uueid', '_', $noiseLevelExpr), SUM(st_area(the_geom)) area, 
               '$uueid', '$period', $noiseLevelExpr, '$cbsType', '$nutsCode', 'R'
        FROM CONTOURING_NOISE_MAP 
        WHERE $filter 
        GROUP BY ISOLABEL;
    """

    new Execute_Query().exec(conn, [sqlQueries: insertSql, outputFormat: "json"], new EmptyProgressVisitor())
}

static String getRoadsLevelsTableName(String posSol) {
    return """ROADS_LEVELS_${posSol.replace("-", "m")}"""
}

private void mergeReceiversLevels(List<String> posSols, Connection h2Connection, String uueid, Logger logger, Sql h2Sql) {
    def posSolsToProcess = new ArrayList<String>(posSols)
    def firstPosSol = posSolsToProcess.pop()
    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, getRoadsLevelsTableName(firstPosSol), "THE_GEOM");
    def mergeLevelsQuery = """
        DROP TABLE IF EXISTS RECEIVERS_LEVEL_$uueid;
        CREATE TABLE RECEIVERS_LEVEL_$uueid(THE_GEOM ${metaData.getSQL()}, IDRECEIVER INTEGER, PERIOD VARCHAR, LAEQ NUMERIC(5, 2));
    """ as String

    mergeLevelsQuery += """
        INSERT INTO RECEIVERS_LEVEL_$uueid SELECT THE_GEOM, IDRECEIVER, PERIOD, LAEQ FROM ${getRoadsLevelsTableName(firstPosSol)};
    """ as String

    posSolsToProcess.each { posSol ->
        mergeLevelsQuery += """
            UPDATE RECEIVERS_LEVEL_$uueid RL SET LAEQ = 10*log10(power(10,RL.LAEQ/10) + power(10,(SELECT LAEQ FROM ${getRoadsLevelsTableName(posSol)} RLS WHERE RL.IDRECEIVER = RLS.IDRECEIVER AND RL.PERIOD = RLS.PERIOD) / 10));
        """ as String
    }

    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", mergeLevelsQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

    logger.info(ScriptUtilities.formatSqlQueryResult(
                    h2Sql, "SELECT * FROM RECEIVERS_LEVEL_$uueid WHERE LAEQ > 0 LIMIT 5" as String, 120))
}

def generateReceivers(Map input,Connection pgConnection,String uueid, Geometry extractionEnvelopeGeometry, Connection h2Connection,double deltaBuildingsReceivers, Map mainConfiguration, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    ProgressVisitor subSteps = stepsProgress.subProcess(4)

    logger.info("Generate receivers on buildings")
    ScriptUtilities.execScript(new Building_Grid(), h2Connection, [tableBuilding: "BUILDINGS", delta: deltaBuildingsReceivers, height: 4.1, distance : 0.1], subSteps)
    Sql h2Sql = new Sql(h2Connection)
    h2Sql.execute("ALTER TABLE RECEIVERS RENAME TO RECEIVERS_BUILDINGS")
    logger.info(ScriptUtilities.formatSqlQueryResult(h2Sql, "SELECT MIN(NBRECEIVERS) MIN_RECEIVERS, AVG(NBRECEIVERS) AVG_RECEIVERS, MAX(NBRECEIVERS) MAX_RECEIVERS, SUM(NBRECEIVERS) ALL_RECEIVERS FROM (SELECT build_pk, COUNT(PK) NBRECEIVERS FROM RECEIVERS_BUILDINGS GROUP BY build_pk)", 120))

    logger.info("Generate Delaunay receivers")
    // Fetch all roads using the UUEID query
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName}
        WHERE uueid = '${uueid}'"""
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
    }

    ScriptUtilities.execScript(new Add_Primary_Key(), h2Connection, [tableName: "ROADS", pkName: "PK"], subSteps)

    def receiversZone = extractionEnvelopeGeometry.buffer(-mainConfiguration.confmaxsrcdist as Double)
    ScriptUtilities.execScript(new Delaunay_Grid(), h2Connection, [
            fence: receiversZone, tableBuilding: "BUILDINGS", sourcesTableName: "ROADS", maxCellDist: 1200,
            skipCellNoSourcesMinimalDistance : 2 * (mainConfiguration.confmaxsrcdist as Double),
            maxArea : 500, height: 4.1, outputTableName: "RECEIVERS_DELAUNAY", isoSurfaceInBuildings : true], subSteps)
}

def fetchDem(Map input, String uueid, String extractionEnvelopeGeometry, Connection h2Connection,Connection pgConnection, ProgressVisitor stepsProgress, String posSol) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch digital elevation model..")
    ProgressVisitor demProgress = stepsProgress.subProcess(5)

    Sql sql = new Sql(h2Connection)
    Sql pgSql = new Sql(pgConnection)

    def fetchTableNamesQuery = """
        SELECT uueid, insee_dep, bd_alti
        FROM cbs_uge_input.nm_link_dept_infra_road_${input.projectionName} nldirh
        WHERE nldirh.uueid = '$uueid';
    """
    def bdAltiTableName = new HashSet<String>()
    pgSql.rows(fetchTableNamesQuery as String).each { row ->
        bdAltiTableName.add(row.bd_alti as String)
    }

    ProgressVisitor subProgress = demProgress.subProcess(bdAltiTableName.size())

    def xyPrecision = 2 // cm precision
    def zPrecision = 2 // cm precision
    bdAltiTableName.forEach { tableName ->
        PostGISUtilities.fetchDemTable(pgConnection, h2Connection, "bd_alti.${tableName}",
                "DEM", extractionEnvelopeGeometry, subProgress, xyPrecision, zPrecision)
    }

    // Enhance DEM points with orography and hydrography ruptures lines
    def tableExt = getDeptCodeFromExt().get(input.projectionName)
    def fetchOroTableQuery = """SELECT st_intersection(geom3d, '$extractionEnvelopeGeometry'::geometry) the_geom
         FROM bd_topo.n_ligne_orographique_bdt_${tableExt}_2023 WHERE ST_Intersects(geom, '$extractionEnvelopeGeometry'::geometry) AND ST_ZMIN(geom3d) > 0"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(fetchOroTableQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "OROGRAPHIC", true, batchSize)
    }


    // Insert lines into DEM table
    def insertOroQuery = """
        INSERT INTO DEM (the_geom) SELECT THE_GEOM FROM ST_Explode('(SELECT ST_TOMULTIPOINT(ST_DENSIFY(the_geom, 5)) THE_GEOM FROM OROGRAPHIC)');
        DROP TABLE OROGRAPHIC;            
        """
    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", insertOroQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

    def fetchHydroTableQuery = """SELECT st_intersection(geom3d, '$extractionEnvelopeGeometry'::geometry) the_geom
         FROM bd_topo.n_troncon_hydrographique_bdt_${tableExt}_2023 WHERE ST_Intersects(geom, '$extractionEnvelopeGeometry'::geometry) AND ST_ZMIN(geom3d) > 0"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(fetchHydroTableQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "HYDROGRAPHIC", true, batchSize)
    }

    // Insert lines into DEM table
    def insertHydroQuery = """
        INSERT INTO DEM (the_geom) SELECT THE_GEOM FROM ST_Explode('(SELECT ST_TOMULTIPOINT(ST_DENSIFY(the_geom, 5)) THE_GEOM FROM HYDROGRAPHIC)');
        DROP TABLE HYDROGRAPHIC;            
        """
    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", insertHydroQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())


    // Fetch road table with altitude using the UUEID query
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName}
        WHERE uueid = '${uueid}' and pos_sol = '$posSol' and (franchisst is null or franchisst = 'Pont')"""
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
    }

    // Create a new DEM with road platforms
    def srid = Generate_sources.getSRIDFromTableExtensionName()[input.projectionName]
    ScriptUtilities.execScript(new Enrich_DEM_with_road(), h2Connection, [inputDEM: "DEM", inputRoad: "ROADS", roadWidth : "WIDTH", outputSuffix: "ENRICHED", inputSRID: srid], demProgress)

    // Replace DEM with the new table
    def replaceDEMQuery = """
        DROP TABLE IF EXISTS DEM;
        ALTER TABLE DEM_ENRICHED RENAME TO DEM;
        """
    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", replaceDEMQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())
}

def runSimulation(Map mainConfiguration, Connection h2Connection, String posSol, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    ScriptUtilities.execScript(new Noise_level_from_source(),
            h2Connection, [
            tableBuilding: "BUILDINGS",
            tableSources: "LW_ROADS",
            tableReceivers: "RECEIVERS_DELAUNAY",
            tableDEM: "DEM",
            tableGroundAbs: "LANDCOVER",
            tablePeriodAtmosphericSettings: "ATMOSPHERIC_SETTINGS",
            confReflOrder: mainConfiguration.confreflorder,
            confMaxSrcDist: mainConfiguration.confmaxsrcdist,
            confMaxReflDist: mainConfiguration.confmaxrefldist,
            confDiffVertical: mainConfiguration.confdiffvertical,
            confDiffHorizontal: mainConfiguration.confdiffhorizontal
            ],
            stepsProgress)
    // Rename output table
    def outputTableName = getRoadsLevelsTableName(posSol)
    Sql h2Sql = new Sql(h2Connection)
    h2Sql.execute("ALTER TABLE RECEIVERS_LEVEL RENAME TO $outputTableName" as String)
}

def processLandCover(Map input,Connection pgConnection, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch land cover..")
    def landCoverQuery = """SELECT geom as the_geom, idnatsol as pk, natsol_lib as clc_lib, natsol_cno as g 
        FROM cbs_uge_input.c_naturesol_${input.projectionName} 
        WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry) AND NATSOL_CNO > 0"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(landCoverQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "LANDCOVER", true, batchSize)
    }
}

/**
 * Fetches atmospheric periods from nearby stations for a given UUEID.
 * @param input The input map containing configuration parameters.
 * @param uueid The road UUEID for which to fetch atmospheric periods.
 * @param h2Connection The H2 database connection.
 * @param stepsProgress The progress visitor for tracking execution progress.
 */
def fetchAtmosphericPeriodFromStations(Map input,Connection pgConnection, String uueid, Connection h2Connection, ProgressVisitor stepsProgress) {
    // tablePeriodAtmosphericSettings — Atmospheric settings table name for each time period
    //
    //    Name of the Atmospheric settings table The table must contain the following columns:
    //
    //        PERIOD : time period (VARCHAR PRIMARY KEY)
    //        WINDROSE : probability of occurrences of favourable propagation conditions (ARRAY(16))
    //        TEMPERATURE : Temperature in celsius (FLOAT)
    //        PRESSURE : air pressure in pascal (FLOAT)
    //        HUMIDITY : air humidity in percentage (FLOAT)
    //        GDISC : choose between accept G discontinuity or not (BOOLEAN) default true
    //        PRIME2520 : choose to use prime values to compute eq. 2.5.20 (BOOLEAN) default false
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch nearest atmospheric points periods for UUEID: {}", uueid)

    def atmosphericQuery = """
        SELECT a.*, ST_Distance(a.the_geom, b.geom) as distance_station
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName} b, cbs_uge_input.nm_stations_${input.projectionName} a
        WHERE UUEID = '$uueid'
        ORDER BY a.the_geom <-> b.geom LIMIT 1
    """
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(atmosphericQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "ATMOSPHERIC", true, batchSize)
    }

    def sql = new Sql(h2Connection)
    def distanceStation = sql.firstRow("SELECT distance_station FROM ATMOSPHERIC")
    logger.info("Distance of ${uueid} to nearest station: ${Math.round(distanceStation.distance_station as double)} m")

    def generateAtmosphericSettingsQuery = """
        DROP TABLE IF EXISTS ATMOSPHERIC_SETTINGS;
        CREATE TABLE ATMOSPHERIC_SETTINGS(
            PERIOD VARCHAR,
            WINDROSE real ARRAY[16],
            TEMPERATURE NUMERIC,
            PRESSURE NUMERIC,
            HUMIDITY NUMERIC,
            GDISC BOOLEAN DEFAULT TRUE,
            PRIME2520 BOOLEAN DEFAULT FALSE
        );
        -- Insert time periods
        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'D', 
            ARRAY[
                /* 0.0°   */ pfav_6_18_0,
                /* 22.5°  */ (0.875 * pfav_6_18_20)  + (0.125 * pfav_6_18_40),
                /* 45.0°  */ (0.75  * pfav_6_18_40)  + (0.25  * pfav_6_18_60),
                /* 67.5°  */ (0.625 * pfav_6_18_60)  + (0.375 * pfav_6_18_80),
                /* 90.0°  */ (0.5   * pfav_6_18_80)  + (0.5   * pfav_6_18_100),
                /* 112.5° */ (0.375 * pfav_6_18_100) + (0.625 * pfav_6_18_120),
                /* 135.0° */ (0.25  * pfav_6_18_120) + (0.75  * pfav_6_18_140),
                /* 157.5° */ (0.125 * pfav_6_18_140) + (0.875 * pfav_6_18_160),
                /* 180.0° */ pfav_6_18_180,
                /* 202.5° */ (0.875 * pfav_6_18_200) + (0.125 * pfav_6_18_220),
                /* 225.0° */ (0.75  * pfav_6_18_220) + (0.25  * pfav_6_18_240),
                /* 247.5° */ (0.625 * pfav_6_18_240) + (0.375 * pfav_6_18_260),
                /* 270.0° */ (0.5   * pfav_6_18_260) + (0.5   * pfav_6_18_280),
                /* 292.5° */ (0.375 * pfav_6_18_280) + (0.625 * pfav_6_18_300),
                /* 315.0° */ (0.25  * pfav_6_18_300) + (0.75  * pfav_6_18_320),
                /* 337.5° */ (0.125 * pfav_6_18_320) + (0.875 * pfav_6_18_340)
            ], 
            temp_6_18, 
            101325, 
            hygro_6_18 * 100 
        FROM ATMOSPHERIC; 
        
        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'E', 
            ARRAY[
                /* 0.0°   */ pfav_18_22_0,
                /* 22.5°  */ (0.875 * pfav_18_22_20)  + (0.125 * pfav_18_22_40),
                /* 45.0°  */ (0.75  * pfav_18_22_40)  + (0.25  * pfav_18_22_60),
                /* 67.5°  */ (0.625 * pfav_18_22_60)  + (0.375 * pfav_18_22_80),
                /* 90.0°  */ (0.5   * pfav_18_22_80)  + (0.5   * pfav_18_22_100),
                /* 112.5° */ (0.375 * pfav_18_22_100) + (0.625 * pfav_18_22_120),
                /* 135.0° */ (0.25  * pfav_18_22_120) + (0.75  * pfav_18_22_140),
                /* 157.5° */ (0.125 * pfav_18_22_140) + (0.875 * pfav_18_22_160),
                /* 180.0° */ pfav_18_22_180,
                /* 202.5° */ (0.875 * pfav_18_22_200) + (0.125 * pfav_18_22_220),
                /* 225.0° */ (0.75  * pfav_18_22_220) + (0.25  * pfav_18_22_240),
                /* 247.5° */ (0.625 * pfav_18_22_240) + (0.375 * pfav_18_22_260),
                /* 270.0° */ (0.5   * pfav_18_22_260) + (0.5   * pfav_18_22_280),
                /* 292.5° */ (0.375 * pfav_18_22_280) + (0.625 * pfav_18_22_300),
                /* 315.0° */ (0.25  * pfav_18_22_300) + (0.75  * pfav_18_22_320),
                /* 337.5° */ (0.125 * pfav_18_22_320) + (0.875 * pfav_18_22_340)
            ], 
            temp_18_22, 
            101325, 
            hygro_18_22 * 100 
        FROM ATMOSPHERIC;    

        INSERT INTO ATMOSPHERIC_SETTINGS (PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY)
        SELECT 
            'N', 
            ARRAY[
                /* 0.0°   */ pfav_22_6_0,
                /* 22.5°  */ (0.875 * pfav_22_6_20)  + (0.125 * pfav_22_6_40),
                /* 45.0°  */ (0.75  * pfav_22_6_40)  + (0.25  * pfav_22_6_60),
                /* 67.5°  */ (0.625 * pfav_22_6_60)  + (0.375 * pfav_22_6_80),
                /* 90.0°  */ (0.5   * pfav_22_6_80)  + (0.5   * pfav_22_6_100),
                /* 112.5° */ (0.375 * pfav_22_6_100) + (0.625 * pfav_22_6_120),
                /* 135.0° */ (0.25  * pfav_22_6_120) + (0.75  * pfav_22_6_140),
                /* 157.5° */ (0.125 * pfav_22_6_140) + (0.875 * pfav_22_6_160),
                /* 180.0° */ pfav_22_6_180,
                /* 202.5° */ (0.875 * pfav_22_6_200) + (0.125 * pfav_22_6_220),
                /* 225.0° */ (0.75  * pfav_22_6_220) + (0.25  * pfav_22_6_240),
                /* 247.5° */ (0.625 * pfav_22_6_240) + (0.375 * pfav_22_6_260),
                /* 270.0° */ (0.5   * pfav_22_6_260) + (0.5   * pfav_22_6_280),
                /* 292.5° */ (0.375 * pfav_22_6_280) + (0.625 * pfav_22_6_300),
                /* 315.0° */ (0.25  * pfav_22_6_300) + (0.75  * pfav_22_6_320),
                /* 337.5° */ (0.125 * pfav_22_6_320) + (0.875 * pfav_22_6_340)
            ], 
            temp_22_6, 
            101325, 
            hygro_22_6 * 100 
        FROM ATMOSPHERIC;

        DROP TABLE ATMOSPHERIC;
    """

    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", generateAtmosphericSettingsQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

    logger.info( ScriptUtilities.formatSqlQueryResult(sql, "SELECT PERIOD, WINDROSE, TEMPERATURE, PRESSURE, HUMIDITY FROM ATMOSPHERIC_SETTINGS", 120))

}

def processRoads(Map input,Connection pgConnection, String uueid, Connection h2Connection, ProgressVisitor stepsProgress, String posSol) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch roads..")
    def roadsQuery = """SELECT * FROM cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid LIKE '$uueid' AND pos_sol='$posSol' AND (franchisst IS NULL OR franchisst = 'Pont')"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadsQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "LW_ROADS", true, batchSize)
    }
}

def processBuildings(Map input,Connection pgConnection, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress, double wallAlpha) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch buildings..")
    def projectionName = input.projectionName
    def tableQuery = """SELECT b.geom3d as the_geom, b.bat_haut as height, b.idbat, p.pop_bat as pop, b.bat_idtopo
             FROM cbs_uge_input.c_batiment_s_${projectionName} b 
                INNER JOIN cbs_uge_input.c_population_${projectionName} p ON b.idbat = p.idbat  
             WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry)"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(tableQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "BUILDINGS", true, batchSize)
    }

    Sql sql = new Sql(h2Connection)
    sql.execute("""ALTER TABLE buildings ADD COLUMN g float DEFAULT $wallAlpha;""" as String)

    def erpsQuery = """SELECT idbat, b.erps_nature from cbs_uge_input.c_batimentsensible_${projectionName} b, cbs_uge_input.c_correspond_batiment_batimentsensible_${projectionName} a  WHERE ST_Intersects(geom3d, '${extractionEnvelopeGeometry}'::geometry) AND a.iderps = b.iderps"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(erpsQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "BUILDINGS_ERPS", true, batchSize)
    }

    def noiseBarrierQuery = """SELECT ST_Force3DZ(ST_CollectionHomogenize(geom)) as the_geom, hauteur as height, propriete, materiau1, idprotacou FROM cbs_uge_input.n_routier_protection_acoustique_hexa AS nrpah WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry)"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(noiseBarrierQuery)) {
        PostGISUtilities.copyFromPostGISToH2Database(pgConnection, rs, h2Connection, "BUILDINGS_BARRIERS", true, batchSize)
    }

    //remove constraint on geometry type
    // add barriers with densification 1 meter (will follow the dem)
    // remove Z when Z altitude is not good (will use HEIGHT field)
    def insertBarriersSql = """

        ALTER TABLE BUILDINGS_BARRIERS ADD COLUMN G float DEFAULT 0;
    
        UPDATE BUILDINGS_BARRIERS SET G = 0.7 WHERE propriete = '01';
        UPDATE BUILDINGS_BARRIERS SET G = 0.7 WHERE (propriete = '00' or propriete = '99') AND (materiau1 = '01' or materiau1 = '04' or materiau1 = '06');
        
        DROP TABLE IF EXISTS BUILDINGS_BARRIER_EXPLODED;
        CREATE TABLE BUILDINGS_BARRIER_EXPLODED AS SELECT * FROM ST_EXPLODE('(SELECT ST_ToMultiSegments(st_densify(the_geom, 1)) the_geom, height, G, idprotacou FROM BUILDINGS_BARRIERS)');
        
        -- Use generic type in order to mix polygon and linestring
        ALTER TABLE BUILDINGS ALTER COLUMN the_geom GEOMETRY;
        -- Add origin column, road is acoustic protection along roads
        ALTER TABLE BUILDINGS ADD COLUMN origin varchar DEFAULT 'building';
        INSERT INTO BUILDINGS(the_geom, height, G, origin, pop, idbat, bat_idtopo) SELECT the_geom, height, G, 'road', 0, '', idprotacou from BUILDINGS_BARRIER_EXPLODED;
        UPDATE BUILDINGS SET THE_GEOM = ST_Force2D(THE_GEOM) 
        WHERE ST_ZMIN(THE_GEOM) < -999 
        OR (ST_ZMIN(THE_GEOM) = 0 AND ST_ZMAX(THE_GEOM) = 0);
        
        DROP TABLE BUILDINGS_BARRIERS, BUILDINGS_BARRIER_EXPLODED;
        """

    new Execute_Query().exec(sql.connection,
            Map.of("sqlQueries", insertBarriersSql, "outputFormat", "json"),
            new EmptyProgressVisitor())

    ScriptUtilities.execScript(new Add_Primary_Key(), h2Connection, [tableName: "BUILDINGS", pkName: "PK"], stepsProgress)
}

static Map getDeptCodeFromExt() {
    return  ["hexa": '000', "guad": '971', "guya": '973', "mart": '972', "reun": '974' ]
}