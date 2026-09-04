package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.Field
import org.h2.value.ValueGeometry
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.GeometryMetaData
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.twkb.TWKBWriter
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.covadis.webserver.utilities.ScriptUtilities
import org.noise_planet.noisemodelling.scripts.Database_Manager.Execute_Query
import org.noise_planet.noisemodelling.scripts.Geometric_Tools.Enrich_DEM_with_road
import org.noise_planet.noisemodelling.webserver.database.DatabaseManagement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

title = 'Expose the enriched DEM of a single UUEID as contour lines'
description = 'Extracts the enriched digital elevation model of a single UUEID (bd_alti points, orographic and hydrographic rupture lines, road platforms) using the same process as ComputePerUUEID and pushes the contour lines to cbs_uge_output.dem_<projection>'

inputs = [
        projectionName  : [
                name         : "Projection name",
                title        : "Projection name",
                description  : "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type         : String.class
        ],
        uueid           : [
                name       : "UUEID",
                title      : "UUEID",
                description: "UUEID of the road infrastructure to process. eg. RD_FR_00_0781651",
                type       : String.class
        ],
        conf            : [
                title      : "Configuration identifier",
                name       : "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type       : Integer.class
        ],
        contourInterval : [
                name       : "Contour interval",
                title      : "Contour interval",
                description: "Vertical distance between two contour lines in meters",
                default    : 1.0,
                type       : Double.class
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

        String uueid = input.uueid as String
        String projectionName = input.projectionName as String

        def tempDirectory = File.createTempDir()
        // Create a local H2 database for this task
        DataSource h2DataSource = DatabaseManagement.createH2DataSource(tempDirectory.getAbsolutePath() ,
                "h2_dem_$uueid", "sa", "sa", "", true)
        logger.info("Create database for UUEID: $uueid in directory: $tempDirectory")
        ProgressVisitor stepsProgress = progress.subProcess(3) // long running sub tasks

        int contourCount = computeForUUEID(uueid, h2DataSource, pgConnection, stepsProgress, input, mainConfiguration)

        // Delete the database file
        if(h2DataSource instanceof Closeable) {
            ((Closeable) h2DataSource).close()
        }
        new File(tempDirectory, "h2_dem_${uueid}.mv.db").delete()

        // Return results
        return [result: "Computed $contourCount contour lines for $uueid in cbs_uge_output.dem_$projectionName"]
    }


}

def computeForUUEID(String uueid, DataSource h2DataSource, Connection pgConnection, ProgressVisitor progress, Map input, Map mainConfiguration) {
    def pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Expose DEM for UUEID: $uueid")
    try(Connection h2Connection = h2DataSource.getConnection()) {
        // Compute the extraction envelope with the same buffer as ComputePerUUEID
        def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(st_convexhull(st_collect(the_geom)), ${
            mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE uueid = '$uueid';""" as String)
        if (res == null) {
            throw new IllegalArgumentException("No match for the provided uueid '$uueid'")
        }

        def extractionEnvelopeGeometryWKT = ValueGeometry.getFromGeometry(res.geomenv as Geometry).string

        fetchDem(input, uueid, extractionEnvelopeGeometryWKT, h2Connection, pgConnection, progress)

        enrichDemWithRoads(input, uueid, h2Connection, pgConnection, progress)

        int contourCount = generateContours(h2Connection, "DEM_ENRICHED", input.getOrDefault("contourInterval", 1.0) as Double)

        uploadDemContours(h2Connection, pgConnection, uueid, input.projectionName as String)

        return contourCount
    }
}

def fetchDem(Map input, String uueid, String extractionEnvelopeGeometry, Connection h2Connection, Connection pgConnection, ProgressVisitor stepsProgress) {
    Sql pgSql = new Sql(pgConnection)
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch digital elevation model..")
    def fetchTableNamesQuery = """
        SELECT uueid, insee_dep, bd_alti
        FROM cbs_uge_input.nm_link_dept_infra_road_${input.projectionName} nldirh
        WHERE nldirh.uueid = '$uueid';
    """
    def bdAltiTableName = new HashSet<String>()
    pgSql.rows(fetchTableNamesQuery as String).each { row ->
        bdAltiTableName.add(row.bd_alti as String)
    }
    if (bdAltiTableName.isEmpty()) {
        throw new IllegalArgumentException("No bd_alti table referenced for the provided uueid '$uueid'")
    }

    ProgressVisitor subProgress = stepsProgress.subProcess(bdAltiTableName.size())

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
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "OROGRAPHIC", true, batchSize)
    }


    // Insert lines into DEM table
    def insertOroQuery = """
        INSERT INTO DEM (the_geom) SELECT THE_GEOM FROM ST_Explode('(SELECT ST_TOMULTIPOINT(ST_DENSIFY(the_geom, 5)) THE_GEOM FROM OROGRAPHIC)');
        DROP TABLE OROGRAPHIC;            
        """
    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", insertOroQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

    def fetchHydroTableQuery = """SELECT st_intersection(geom3d, '$extractionEnvelopeGeometry'::geometry) the_geom
         FROM bd_topo.n_troncon_hydrographique_bdt_${tableExt}_2023 WHERE ST_Intersects(geom, '$extractionEnvelopeGeometry'::geometry) AND ST_ZMIN(geom3d) > 0 AND position_par_rapport_au_sol = '0'"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(fetchHydroTableQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "HYDROGRAPHIC", true, batchSize)
    }

    // Insert lines into DEM table
    def insertHydroQuery = """
        INSERT INTO DEM (the_geom) SELECT THE_GEOM FROM ST_Explode('(SELECT ST_TOMULTIPOINT(ST_DENSIFY(the_geom, 5)) THE_GEOM FROM HYDROGRAPHIC)');
        DROP TABLE HYDROGRAPHIC;            
        """
    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", insertHydroQuery, "outputFormat", "json"),
            new EmptyProgressVisitor())

}

def enrichDemWithRoads(Map input, String uueid, Connection h2Connection, Connection pgConnection, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Adapting digital elevation model with road platforms..")
    ProgressVisitor demProgress = stepsProgress.subProcess(2)

    // Fetch all roads of the UUEID (all pos_sol, bridges included) in order to build every road platform at once
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName}
        WHERE uueid = '$uueid' and (franchisst is null or franchisst = 'Pont')"""
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
        demProgress.endStep()
    }

    // Create a new DEM with road platforms
    def srid = Generate_sources.getSRIDFromTableExtensionName()[input.projectionName]
    ScriptUtilities.execScript(new Enrich_DEM_with_road(), h2Connection, [inputDEM: "DEM", inputRoad: "ROADS", roadWidth : "WIDTH", outputSuffix: "ENRICHED", inputSRID: srid], demProgress)
}

/**
 * Compute the contour lines of a DEM points table using the H2GIS triangulation and contouring functions.
 * The resulting LINESTRING lines (with their contour level in the IDISO column) are stored in the DEM_CONTOURS table.
 * @param h2Connection Local H2 connection
 * @param demTable DEM points table name
 * @param interval Vertical distance between two contour lines
 * @return Number of computed contour lines
 */
static int generateContours(Connection h2Connection, String demTable, double interval) {
    Sql h2Sql = new Sql(h2Connection)
    if (!JDBCUtilities.tableExists(h2Connection, demTable) || JDBCUtilities.getRowCount(h2Connection, demTable) == 0) {
        throw new IllegalArgumentException("The DEM table $demTable does not exist or is empty")
    }
    def zRange = h2Sql.firstRow("SELECT MIN(ST_ZMIN(THE_GEOM)) ZMIN, MAX(ST_ZMAX(THE_GEOM)) ZMAX FROM $demTable" as String)
    if (zRange == null || zRange.ZMIN == null) {
        throw new IllegalArgumentException("The DEM table $demTable does not contain 3D points")
    }
    double zmin = zRange.ZMIN as double
    double zmax = zRange.ZMAX as double
    if (zmin == zmax) {
        throw new IllegalArgumentException("The DEM table $demTable is flat, no contour line can be computed")
    }
    int firstLevel = (int) Math.ceil(zmin / interval)
    int lastLevel = (int) Math.floor(zmax / interval)
    if (firstLevel > lastLevel) {
        throw new IllegalArgumentException("The contour interval $interval m is larger than the elevation range of the DEM table $demTable")
    }
    List<Double> levels = new ArrayList<>()
    for (int level = firstLevel; level <= lastLevel; level++) {
        levels.add(level * interval)
    }

    // Triangulate the DEM points (the Z ordinate of the points is preserved by the Delaunay triangulation)
    new Execute_Query().exec(h2Connection, [sqlQueries: """
        DROP TABLE IF EXISTS DEM_TIN;
        CREATE TABLE DEM_TIN AS SELECT THE_GEOM FROM ST_Explode('(SELECT ST_Delaunay(ST_Accum(THE_GEOM)) THE_GEOM FROM $demTable)');
    """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    // Split the triangles along the requested contour levels. The levels are offset by a tiny epsilon so that no
    // triangle vertex (centimeter precision) falls exactly on a level, otherwise ST_TriangleContouring skips the
    // contours of the adjacent intervals. Each output polygon (IDISO = interval index) carries an edge on the
    // contour line of the level that closes its interval
    double levelEpsilon = interval * 1e-6
    List<Double> contourLevels = levels.collect { it + levelEpsilon }
    new Execute_Query().exec(h2Connection, [sqlQueries: """
        DROP TABLE IF EXISTS DEM_CONTOUR_POLYGONS;
        CREATE TABLE DEM_CONTOUR_POLYGONS AS SELECT * FROM ST_TriangleContouring('DEM_TIN', ${contourLevels.join(", ")});
    """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    // Map the interval index (IDISO) to the corresponding contour level
    StringBuilder levelValues = new StringBuilder()
    levels.eachWithIndex { Double level, int idiso ->
        if (levelValues.length() > 0) {
            levelValues.append(", ")
        }
        levelValues.append("($idiso, ${level})")
    }
    new Execute_Query().exec(h2Connection, [sqlQueries: """
        DROP TABLE IF EXISTS DEM_ISO_LEVELS;
        CREATE TABLE DEM_ISO_LEVELS(IDISO SMALLINT, ZLEVEL DOUBLE);
        INSERT INTO DEM_ISO_LEVELS VALUES $levelValues;
        -- Keep only the segments lying on a contour level (both endpoints at the level altitude)
        DROP TABLE IF EXISTS DEM_CONTOURS_SEG;
        CREATE TABLE DEM_CONTOURS_SEG AS SELECT S.THE_GEOM THE_GEOM, L.ZLEVEL IDISO
            FROM ST_Explode('(SELECT ST_ToMultiSegments(ST_Boundary(THE_GEOM)) THE_GEOM, IDISO FROM DEM_CONTOUR_POLYGONS)') S
            INNER JOIN DEM_ISO_LEVELS L ON S.IDISO = L.IDISO
            WHERE ABS(ST_ZMIN(S.THE_GEOM) - (L.ZLEVEL + $levelEpsilon)) < ${levelEpsilon / 2} AND ABS(ST_ZMAX(S.THE_GEOM) - (L.ZLEVEL + $levelEpsilon)) < ${levelEpsilon / 2};
        -- Merge the segments of each level into full contour lines (ST_Union removes the segments shared by adjacent polygons)
        DROP TABLE IF EXISTS DEM_CONTOURS;
        CREATE TABLE DEM_CONTOURS AS SELECT * FROM ST_Explode('(SELECT ST_LineMerge(ST_Union(ST_Accum(THE_GEOM))) THE_GEOM, IDISO FROM DEM_CONTOURS_SEG GROUP BY IDISO)');
    """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    return JDBCUtilities.getRowCount(h2Connection, "DEM_CONTOURS")
}

/**
 * Upload the contour lines of the DEM_CONTOURS table to PostGIS
 * @param h2Connection Local h2 connection
 * @param pgConnection Remote PostGIS connection
 * @param uueid Infrastructure identifier
 * @param projectionName Projection name ex: hexa
 * @return Number of uploaded contour lines
 */
static int uploadDemContours(Connection h2Connection, Connection pgConnection, String uueid, String projectionName) {
    Sql pgSql = new Sql(pgConnection)
    Sql h2Sql = new Sql(h2Connection)
    int batchSize = 100

    // Use the SRID of the original DEM table (the contour geometries generated by H2GIS do not carry a SRID)
    GeometryMetaData demMetaData = GeometryTableUtilities.getMetaData(h2Connection, "DEM", "THE_GEOM")
    int srid = demMetaData.getSRID()

    def tableName = "cbs_uge_output.dem_$projectionName"

    if (!JDBCUtilities.tableExists(pgConnection, tableName)) {
        new Execute_Query().exec(pgConnection, [sqlQueries: """
            CREATE TABLE $tableName (
                the_geom geometry(LINESTRINGZ, $srid),
                uueid varchar not null,
                idiso double precision not null);
            CREATE INDEX ON $tableName USING GIST (the_geom);
            CREATE INDEX ON $tableName (uueid);
            ALTER TABLE $tableName OWNER TO cbs_uge_group;
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())
    }

    new Execute_Query().exec(pgConnection, [sqlQueries: """
            DELETE FROM $tableName WHERE uueid = '$uueid';
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    def insertSql = """
        INSERT INTO $tableName
        (the_geom, uueid, idiso) 
        VALUES (ST_GeomFromTWKB(?), ?, ?)
    """ as String

    TWKBWriter twkbWriter = new TWKBWriter()
    twkbWriter.setEncodeZ(true)
    twkbWriter.setXYPrecision(2)
    twkbWriter.setZPrecision(2)
    pgSql.withBatch(batchSize, insertSql) {
        h2Sql.eachRow("SELECT the_geom, idiso FROM DEM_CONTOURS") { row ->
            // Insert into PostGIS table
            it.addBatch(twkbWriter.write(row.getObject("the_geom") as Geometry),
                    uueid,
                    row.getDouble("idiso"))
        }
    }

    return JDBCUtilities.getRowCount(h2Connection, "DEM_CONTOURS")
}

static Map getDeptCodeFromExt() {
    return  ["hexa": '000', "guad": '971', "guya": '973', "mart": '972', "reun": '974' ]
}
