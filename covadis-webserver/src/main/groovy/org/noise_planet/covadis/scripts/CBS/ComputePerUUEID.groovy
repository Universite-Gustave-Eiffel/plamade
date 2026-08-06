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

    fetchDem(input, uueid, extractionEnvelopeGeometryWKT, h2Connection, pgConnection, stepsProgress)

    fetchBuildings(input, pgConnection, extractionEnvelopeGeometryWKT, h2Connection, stepsProgress, mainConfiguration.wall_alpha as Double)

    generateReceivers(input, pgConnection, uueid, extractionEnvelopeGeometry, h2Connection,
            mainConfiguration.confdistbuildingsreceivers as Double, mainConfiguration, stepsProgress)

    processLandCover(input, pgConnection, extractionEnvelopeGeometryWKT, h2Connection, stepsProgress)

    fetchAtmosphericPeriodFromStations(input, pgConnection, uueid, h2Connection, stepsProgress)

    def posSolQuery = """SELECT DISTINCT pos_sol FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE (franchisst IS NULL OR franchisst = 'Pont')""" as String

    def posSols = pgSql.rows(posSolQuery).collect { it.pos_sol as String}

    ProgressVisitor solProgress = stepsProgress.subProcess(posSols.size())

    new ArrayList<>(posSols).forEach {posSol ->
        logger.info("Compute for pos_sol = $posSol")
        // Fetch specific road emission at this special height
        if(fetchRoads(input, pgConnection, uueid, h2Connection, solProgress, posSol, mainConfiguration.confmaxsrcdist * 1.2d as Double)) {
            // Adapt the DEM with this special road height platforms
            enrichDem(input, uueid, h2Connection, pgConnection, solProgress, posSol)
            // Run the simulation with this road height
            runSimulation(mainConfiguration, h2Connection, posSol, solProgress)
        } else {
            logger.info("Skip pos_sol {}", posSol)
            posSols.removeElement(posSol)
        }
    }

    // Merge noise levels for each pos sols
    mergeReceiversLevels(posSols, h2Connection, uueid, logger, h2Sql)

    // Generate IsoContours
    generateRoadsCBS(h2Connection, uueid, stepsProgress, codeDeptToNuts)

    generateBuildingsFacadeExpo(h2Connection, uueid, codeDeptToNuts)
    generateExposureStatisticsFromFacadeExpo(h2Connection, uueid, codeDeptToNuts, input.projectionName as String)

    // Upload CBS Table to remote PostGIS database
    uploadCBS(h2Connection, pgConnection, uueid, input.projectionName as String)

    uploadFacadeExpo(h2Connection, pgConnection, uueid, input.projectionName as String)

}

/**
 * Upload the content of the facade exposure table to PostGIS
 * @param h2Connection Local h2 connection
 * @param pgConnection Remote PostGIS connection
 * @param uueid Infrastructure identifier
 * @param projectionName Projection name ex: hexa
 */
def uploadFacadeExpo(Connection h2Connection, Connection pgConnection, String uueid, String projectionName) {
    boolean tableExists = JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.facade_expo_$projectionName")
    if(tableExists) {
        new Execute_Query().exec(pgConnection, [sqlQueries: """
            DELETE FROM cbs_uge_output.facade_expo_$projectionName WHERE uueid = '$uueid';
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())
    }

    try( Statement st = h2Connection.createStatement() ;
         ResultSet rs = st.executeQuery("""SELECT the_geom, idbat , uueid , lden , ln FROM FACADE_EXPO""")) {
        PostGISUtilities.copyResultSetToDatabase(h2Connection, rs, pgConnection,
                "cbs_uge_output.facade_expo_$projectionName", false, batchSize)
    }

    if(!tableExists) {
        // Create index
        new Execute_Query().exec(pgConnection, [sqlQueries: """            
            CREATE INDEX ON cbs_uge_output.facade_expo_$projectionName USING GIST (the_geom);
            CREATE INDEX ON cbs_uge_output.facade_expo_$projectionName (uueid);
            CREATE INDEX ON cbs_uge_output.facade_expo_$projectionName (idbat);
            ALTER TABLE cbs_uge_output.facade_expo_$projectionName OWNER TO cbs_uge_group;
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())

    }

    tableExists = JDBCUtilities.tableExists(pgConnection, "cbs_uge_output.expo_$projectionName")
    if(tableExists) {
        new Execute_Query().exec(pgConnection, [sqlQueries: """
            DELETE FROM cbs_uge_output.expo_$projectionName WHERE uueid = '$uueid';
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())
    }
    try( Statement st = h2Connection.createStatement() ;
         ResultSet rs = st.executeQuery("""SELECT PK, NUTSCODE , UUEID, NOISELEVEL, ROUND(PEOPLE)::integer PEOPLE,
                 ROUND(DWELLINGS)::integer DWELLINGS, HOSPITALS , SCHOOLS , CPI , HA  , HSD , AREA , INDICETYPE 
                 FROM EXPO_${projectionName}""")) {
        PostGISUtilities.copyResultSetToDatabase(h2Connection, rs, pgConnection,
                "cbs_uge_output.expo_$projectionName", false, batchSize)
    }
    if(!tableExists) {
        // Create index
        new Execute_Query().exec(pgConnection, [sqlQueries: """            
            CREATE INDEX ON cbs_uge_output.expo_$projectionName (uueid);
            CREATE INDEX ON cbs_uge_output.expo_$projectionName (NUTSCODE);
            ALTER TABLE cbs_uge_output.expo_$projectionName ALTER COLUMN pk SET NOT NULL;
            ALTER TABLE cbs_uge_output.expo_$projectionName ADD PRIMARY KEY (pk);
            ALTER TABLE cbs_uge_output.expo_$projectionName OWNER TO cbs_uge_group;
        """ as String, outputFormat: "json"], new EmptyProgressVisitor())
    }
}


static def generateExposureStatisticsFromFacadeExpo(Connection h2Connection, String uueid, Map<String, String> codeDeptToNuts, String projectionName) {

    Logger logger = LoggerFactory.getLogger(this.class)
    // 1. Extract metadata
    def codeDept = uueid.split("_")[3].substring(0, 3)
    def nutsCode = codeDeptToNuts.get(codeDept)


    runScript(h2Connection, """

        -- Compute the rank for each point among others of the same building
        ALTER TABLE FACADE_EXPO ADD COLUMN rank_lden DOUBLE;
        ALTER TABLE FACADE_EXPO ADD COLUMN rank_ln DOUBLE;
        MERGE INTO FACADE_EXPO AS t
        USING (
          SELECT _ROWID_ AS rid,
                 -- Formula: (Row_Number - 1) / (Total_Rows - 1)
                 -- NULLIF protects against Division by Zero if a building has only 1 point
                 CAST(ROW_NUMBER() OVER (PARTITION BY pkbat ORDER BY lden DESC) - 1 AS DOUBLE) / 
                   NULLIF(COUNT(*) OVER (PARTITION BY pkbat) - 1, 0) AS calculated_lden,
                 
                 CAST(ROW_NUMBER() OVER (PARTITION BY pkbat ORDER BY ln DESC) - 1 AS DOUBLE) / 
                   NULLIF(COUNT(*) OVER (PARTITION BY pkbat) - 1, 0) AS calculated_ln
          FROM FACADE_EXPO
        ) AS s
        ON t._ROWID_ = s.rid
        WHEN MATCHED THEN
          UPDATE SET t.rank_lden = COALESCE(s.calculated_lden, 0.0),
                     t.rank_ln = COALESCE(s.calculated_ln, 0.0);
        -- compute max level per building
        DROP TABLE IF EXISTS FACADE_EXPO_MAX_LEVEL;
        CREATE TABLE FACADE_EXPO_MAX_LEVEL AS 
            SELECT B.IDBAT, B.erps_nature, COALESCE(B.POP, 0) POP, MAX(LDEN) LDEN, MAX(LN) LN  
            FROM FACADE_EXPO F INNER JOIN BUILDINGS B ON ( F.pkbat = B.pk )
            WHERE B.erps_nature IS NOT NULL or B.nb_logts_c = 1 GROUP BY B.IDBAT, B.erps_nature, B.POP;
        -- Create range tables
        DROP TABLE IF EXISTS ROAD_NOISE_LEVEL_RANGES;
        CREATE TABLE ROAD_NOISE_LEVEL_RANGES(cbstype varchar, period varchar, noiselevel_start numeric(5,2), noiselevel_end numeric(5,2), noiselevel varchar);
        -- LDEN Period - Type A
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LD', 55, 60, 'Lden5559');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LD', 60, 65, 'Lden6064');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LD', 65, 70, 'Lden6569');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LD', 70, 75, 'Lden7074');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LD', 75, 200, 'LdenGreaterThan75');
        
        -- LDEN Period - Type C
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('C', 'LD', 68, 200, 'LdenGreaterThan68');
        
        -- LN Period - Type A
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LN', 50, 55, 'Lnight5054');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LN', 55, 60, 'Lnight5559');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LN', 60, 65, 'Lnight6064');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LN', 65, 70, 'Lnight6569');
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('A', 'LN', 70, 200, 'LnightGreaterThan70');
        
        -- LN Period - Type C
        INSERT INTO ROAD_NOISE_LEVEL_RANGES (cbstype, period, noiselevel_start, noiselevel_end, noiselevel) VALUES ('C', 'LN', 62, 200, 'LnightGreaterThan62');
        -- Create main exposure table to upload
        DROP TABLE IF EXISTS EXPO_${projectionName};
        CREATE TABLE EXPO_${projectionName}(pk varchar not null primary key, nutscode varchar, uueid varchar, noiselevel varchar, people double,
         dwellings double, hospitals int, schools int, cpi int, ha int, hsd int, area float, indicetype varchar, noiselevel_start numeric(5,2), noiselevel_end numeric(5,2));
        -- Fill with default values
        INSERT INTO EXPO_${projectionName}
         SELECT CONCAT('${uueid}','_', noiselevel) pk, '${nutsCode}', '${uueid}', noiselevel, 0, 0, 0, 0, 0, 0, 0, 0.0,
             period, noiselevel_start, noiselevel_end
         FROM ROAD_NOISE_LEVEL_RANGES WHERE cbstype = 'A';
        -- Update individual dwellings/schools/hospitals count from FACADE_EXPO_MAX_LEVEL table
        UPDATE EXPO_${projectionName} SET dwellings = dwellings
             + (SELECT COUNT(*) FROM FACADE_EXPO_MAX_LEVEL FL WHERE 
                 ((indicetype = 'LD' AND FL.LDEN >= noiselevel_start AND FL.LDEN < noiselevel_end) OR
                 (indicetype = 'LN' AND FL.LN >= noiselevel_start AND FL.LN < noiselevel_end)) AND POP > 0),
                 hospitals = hospitals
             + (SELECT COUNT(*) FROM FACADE_EXPO_MAX_LEVEL FL WHERE 
                 ((indicetype = 'LD' AND FL.LDEN >= noiselevel_start AND FL.LDEN < noiselevel_end) OR
                 (indicetype = 'LN' AND FL.LN >= noiselevel_start AND FL.LN < noiselevel_end)) AND erps_nature = 'santé et social'),
                 schools = schools
             + (SELECT COUNT(*) FROM FACADE_EXPO_MAX_LEVEL FL WHERE 
                 ((indicetype = 'LD' AND FL.LDEN >= noiselevel_start AND FL.LDEN < noiselevel_end) OR
                 (indicetype = 'LN' AND FL.LN >= noiselevel_start AND FL.LN < noiselevel_end)) AND erps_nature = 'Enseignement'),
                 people = people
             + (SELECT COALESCE(SUM(pop), 0) FROM FACADE_EXPO_MAX_LEVEL FL WHERE 
                 ((indicetype = 'LD' AND FL.LDEN >= noiselevel_start AND FL.LDEN < noiselevel_end) OR
                 (indicetype = 'LN' AND FL.LN >= noiselevel_start AND FL.LN < noiselevel_end)) AND erps_nature is null);
        -- Update collective dwellings people using the lden and ln rank (keeping 50% of most exposed receivers)        
        UPDATE EXPO_${projectionName} SET people = people 
             + COALESCE((SELECT sum(b.POP::float/(select count(*) from FACADE_EXPO AFE where AFE.rank_lden <= 0.5 and AFE.pkbat=FE.pkbat)) popshare
              FROM FACADE_EXPO FE INNER JOIN BUILDINGS B ON (FE.pkbat = B.pk) 
              WHERE NB_LOGTS_C > 1 and pop > 0 and FE.rank_lden <= 0.5 and
               indicetype = 'LD' AND FE.LDEN >= noiselevel_start AND FE.LDEN < noiselevel_end), 0);
        UPDATE EXPO_${projectionName} SET people = people 
             + COALESCE((SELECT sum(b.POP::float/(select count(*) from FACADE_EXPO AFE where AFE.rank_ln <= 0.5 and AFE.pkbat=FE.pkbat)) popshare
              FROM FACADE_EXPO FE INNER JOIN BUILDINGS B ON (FE.pkbat = B.pk) 
              WHERE NB_LOGTS_C > 1 and pop > 0 and FE.rank_ln <= 0.5 and
               indicetype = 'LN' AND FE.LN >= noiselevel_start AND FE.LN < noiselevel_end), 0);
        -- Update collective dwellings appartments using the lden and ln rank (keeping 50% of most exposed receivers)        
        UPDATE EXPO_${projectionName} SET dwellings = dwellings 
             + COALESCE((SELECT sum(b.NB_LOGTS_C::float/(select count(*) from FACADE_EXPO AFE where AFE.rank_lden <= 0.5 and AFE.pkbat=FE.pkbat)) popshare
              FROM FACADE_EXPO FE INNER JOIN BUILDINGS B ON (FE.pkbat = B.pk) 
              WHERE NB_LOGTS_C > 1 and pop > 0 and FE.rank_lden <= 0.5 and
               indicetype = 'LD' AND FE.LDEN >= noiselevel_start AND FE.LDEN < noiselevel_end), 0);
        UPDATE EXPO_${projectionName} SET dwellings = dwellings 
             + COALESCE((SELECT sum(b.NB_LOGTS_C::float/(select count(*) from FACADE_EXPO AFE where AFE.rank_ln <= 0.5 and AFE.pkbat=FE.pkbat)) popshare
              FROM FACADE_EXPO FE INNER JOIN BUILDINGS B ON (FE.pkbat = B.pk) 
              WHERE NB_LOGTS_C > 1 and pop > 0 and FE.rank_ln <= 0.5 and
               indicetype = 'LN' AND FE.LN >= noiselevel_start AND FE.LN < noiselevel_end), 0);
        """)

    logger.info(ScriptUtilities.formatSqlQueryResult(new Sql(h2Connection), """SELECT * FROM EXPO_${projectionName}""" as String, 120))
}

def generateBuildingsFacadeExpo(Connection h2Connection, String uueid, Map<String, String> codeDeptToNuts) {

    Logger logger = LoggerFactory.getLogger(this.class)

    // 1. Extract metadata
    def codeDept = uueid.split("_")[3].substring(0, 3)
    def nutsCode = codeDeptToNuts.get(codeDept)
    logger.info("Processing Exposure uueid: $uueid, codeDept: $codeDept, nutsCode: $nutsCode")

    // RECEIVERS_LEVEL_$uueid
    def receiversLevelTable = "RECEIVERS_LEVEL_$uueid"

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, receiversLevelTable, "THE_GEOM");

    runScript(h2Connection, """
        DROP TABLE IF EXISTS FACADE_EXPO;
        CREATE TABLE FACADE_EXPO (THE_GEOM ${metaData.getSQL()}, idbat varchar(32), pkbat int, uueid varchar, lden numeric(5,2), ln numeric(5,2));
        SET @LASTDELAUNAY=(SELECT MAX(PK) FROM RECEIVERS_DELAUNAY);
        -- Receivers from delaunay triangulation and on buildings are merged, we used the last delaunay
        -- primary key to find the appropriate index from the RECEIVERS_BUILDINGS table
        -- Generate exposition per receiver on each building
        INSERT INTO FACADE_EXPO (THE_GEOM, idbat, pkbat, uueid, lden, ln)        
        SELECT 
            RL.THE_GEOM, 
            B.IDBAT,
            B.pk pkbat, 
            '$uueid' AS UUEID,
            RL.LDEN,
            RL.LN
        FROM PUBLIC.RECEIVERS R
        INNER JOIN PUBLIC.RECEIVERS_BUILDINGS RB ON (R.PK - @LASTDELAUNAY) = RB.PK
        INNER JOIN PUBLIC.BUILDINGS B ON RB.BUILD_PK = B.PK
        INNER JOIN (
            SELECT 
                IDRECEIVER,
                MAX(CASE WHEN PERIOD = 'DEN' THEN THE_GEOM END) AS THE_GEOM,
                MAX(CASE WHEN PERIOD = 'DEN' THEN LAEQ END) AS LDEN,
                MAX(CASE WHEN PERIOD = 'N' THEN LAEQ END) AS LN,
                MAX(LAEQ) as MAX_LAEQ
            FROM PUBLIC.RECEIVERS_LEVEL_$uueid
            GROUP BY IDRECEIVER
        ) RL ON RL.IDRECEIVER = R.PK
        WHERE R.PK > @LASTDELAUNAY
          AND RL.MAX_LAEQ > 0;
    """)
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
                uueid varchar not null,
                noiselevel varchar);
            ALTER TABLE cbs_uge_output.CBS_$projectionName ADD CONSTRAINT cbs_pk_$projectionName PRIMARY KEY (pk);
            CREATE INDEX ON cbs_uge_output.CBS_$projectionName USING GIST (the_geom);
            CREATE INDEX ON cbs_uge_output.CBS_$projectionName (uueid);
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
    setupResultTables(h2Connection, uueid)

    // 3. Define the Noise Level CASE statements for CBS Type A
    def caseLdenA = "(CASE WHEN ISOLABEL = '55-60' THEN 'Lden5559' WHEN ISOLABEL = '60-65' THEN 'Lden6064' WHEN ISOLABEL = '65-70' THEN 'Lden6569' WHEN ISOLABEL = '70-75' THEN 'Lden7074' WHEN ISOLABEL = '75+' THEN 'LdenGreaterThan75' END)"
    def caseLnightA = "(CASE WHEN ISOLABEL = '50-55' THEN 'Lnight5054' WHEN ISOLABEL = '55-60' THEN 'Lnight5559' WHEN ISOLABEL = '60-65' THEN 'Lnight6064' WHEN ISOLABEL = '65-70' THEN 'Lnight6569' WHEN ISOLABEL = '70+' THEN 'LnightGreaterThan70' END)"

    // 4. Generate the 4 CBS Maps


    new Execute_Query().exec(h2Connection, [sqlQueries: "DROP TABLE IF EXISTS ISOPHONES;", outputFormat: "json"], new EmptyProgressVisitor())

    // CBS A - Day/Evening/Night
    processIsoContouring(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_DEN_$uueid", "55.0,60.0,65.0,70.0,75.0,200.0", caseLdenA, "LD", "A", "ISOLVL > 0")

    // CBS A - Night
    processIsoContouring(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_NIGHT_$uueid", "50.0,55.0,60.0,65.0,70.0,200.0", caseLnightA, "LN", "A", "ISOLVL > 0")

    // CBS C - Day/Evening/Night
    processIsoContouring(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_DEN_$uueid", "68.0,200.0", "'LdenGreaterThan68'", "LD", "C", "ISOLVL = 1")

    // CBS C - Night
    processIsoContouring(h2Connection, stepsProgress, uueid, nutsCode, "RECEIVERS_LEVEL_NIGHT_$uueid", "62.0,200.0", "'LdenGreaterThan62'", "LN", "C", "ISOLVL = 1")
}

/**
 * <p>This function creates temporary tables for DEN (Day/Evening/Night) and N (Night) periods
 * by splitting the data from RECEIVERS_LEVEL_$uueid into separate tables with primary keys.</p>
 *
 * @param h2Connection The database connection
 * @param uueid The UUEID identifier used to qualify table names
 */
private void setupResultTables(Connection h2Connection, String uueid) {
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

static def generateIsoClassSql(String fieldName, String rangeStr) {
    // Convert '55.0,60.0...' into a list of Doubles
    List<Double> levels = rangeStr.split(',').collect { it.toDouble() }

    // Helper to format numbers (remove .0 if not needed)
    def fmt = { Double d -> d % 1 == 0 ? d.toInteger().toString() : d.toString() }

    StringBuilder sql = new StringBuilder("CASE ")

    for (int i = 0; i < levels.size(); i++) {
        double current = levels[i]

        if (i == 0) {
            // Case for values lower than the first threshold: e.g. < 55 -> '-55'
            sql.append("WHEN $fieldName < $current THEN '-${fmt(current)}' ")
        }

        // Ranges between thresholds: e.g. >= 55 AND < 60 -> '55-60'
        if (i > 0 && i < levels.size()) {
            double prev = levels[i-1]
            String label

            if (i == levels.size() - 1) {
                label = "${fmt(prev)}+"
                sql.append("ELSE '$label' ")
            } else {
                label = "${fmt(prev)}-${fmt(current)}"
                sql.append("WHEN $fieldName >= $prev AND $fieldName < $current THEN '$label' ")
            }
        }
    }
    sql.append("END")
    return sql.toString()
}

/**
 * Main sub-function to process Isosurfaces and Insert into ISOPHONES
 */
private static void processIsoContouring(Connection conn, ProgressVisitor progress, String uueid, String nutsCode, String sourceTable, String isoClass, String noiseLevelExpr, String period, String cbsType, String filter) {
    Sql h2Sql = new Sql(conn)
    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(conn, sourceTable, "THE_GEOM");
    // Initialize ISOPHONES table if not exists
    new Execute_Query().exec(conn, [sqlQueries: """CREATE TABLE IF NOT EXISTS ISOPHONES
                (the_geom GEOMETRY(MULTIPOLYGONZ, ${metaData.getSRID()}), pk varchar not null , UUEID varchar,
                 PERIOD varchar, NOISELEVEL varchar, AREA float, cbstype varchar, nutscode varchar, typesource varchar);
                """ as String, outputFormat: "json"], new EmptyProgressVisitor())


    // Execute Isosurface creation for standard receivers
    ScriptUtilities.execScript(new Create_Isosurface(), conn, [
            resultTable: sourceTable,
            smoothCoefficient: 0,
            isoClass: isoClass
    ], progress)

    // For IsoSurface convert in sql the LAEQ value into the expected ISOLABEL that should be produced by Create_Isosurface block
    def caseWhenSql = generateIsoClassSql("MIN(LAEQ)", isoClass)

    // Insert results into ISOPHONES
    new Execute_Query().exec(conn, [sqlQueries: """
        -- Fetch the minimum level for each buildings
        DROP TABLE IF EXISTS BUILDINGS_MINIMUM_LEVEL;
        CREATE TABLE BUILDINGS_MINIMUM_LEVEL(build_pk int not null primary key, isolabel varchar) AS SELECT build_pk, $caseWhenSql MIN_LAEQ FROM $sourceTable S 
            INNER JOIN TRIANGLES_OVER_BUILDINGS T ON (S.IDRECEIVER = T.PK_1 OR S.IDRECEIVER = T.PK_2 OR 
            S.IDRECEIVER = T.PK_3) GROUP BY build_pk;
        -- Merge current isocontour with the triangles under buildings using the isolabel value extracted from the LAEQ of all the receivers of the same building
        INSERT INTO CONTOURING_NOISE_MAP(THE_GEOM, ISOLABEL, ISOLVL) SELECT THE_GEOM, (SELECT isolabel FROM BUILDINGS_MINIMUM_LEVEL WHERE build_pk = T.build_pk), 99 FROM TRIANGLES_OVER_BUILDINGS T;
        -- Insert standard isophones value from contouring noise map
        INSERT INTO ISOPHONES(the_geom, pk, area, uueid, period, noiselevel, cbstype, nutscode, typesource) 
        SELECT ST_Multi(ST_Union(ST_Accum(THE_GEOM))) THE_GEOM, concat('$uueid', '_', $noiseLevelExpr), SUM(st_area(the_geom)) area, 
               '$uueid', '$period', $noiseLevelExpr, '$cbsType', '$nutsCode', 'R'
        FROM CONTOURING_NOISE_MAP 
        WHERE $filter 
        GROUP BY ISOLABEL;
        -- Remove isophones with null values in noiselevel
        DELETE FROM ISOPHONES WHERE noiselevel IS NULL;
    """ as String, outputFormat: "json"], new EmptyProgressVisitor())
}

static String getRoadsLevelsTableName(String posSol) {
    return """ROADS_LEVELS_${posSol.replace("-", "m")}"""
}

/**
 * Merge levels of all pos_sol into a single result table
 * @param posSols List of pos_sol to merge
 * @param h2Connection h2 database connection
 * @param uueid Infrastructure identifier
 * @param logger Logger instance
 * @param h2Sql h2 SQL instance
 */
private void mergeReceiversLevels(List<String> posSols, Connection h2Connection, String uueid, Logger logger, Sql h2Sql) {
    def posSolsToProcess = new ArrayList<String>(posSols)
    def firstPosSol = posSolsToProcess.pop()
    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, getRoadsLevelsTableName(firstPosSol), "THE_GEOM");
    def mergeLevelsQuery = """
        DROP TABLE IF EXISTS RECEIVERS_LEVEL_$uueid;
        CREATE TABLE RECEIVERS_LEVEL_$uueid(THE_GEOM ${metaData.getSQL()}, IDRECEIVER INTEGER, PERIOD VARCHAR, LAEQ NUMERIC(5, 2) NOT NULL);
    """ as String

    mergeLevelsQuery += """
        INSERT INTO RECEIVERS_LEVEL_$uueid SELECT THE_GEOM, IDRECEIVER, PERIOD, LAEQ FROM ${getRoadsLevelsTableName(firstPosSol)};
    """ as String

    posSolsToProcess.each { posSol ->
        // update existing rows then insert new rows
        mergeLevelsQuery += """
            SELECT COUNT(*) FROM RECEIVERS_LEVEL_$uueid;
            UPDATE RECEIVERS_LEVEL_$uueid RL SET LAEQ = 10*log10(power(10,RL.LAEQ/10) + power(10,(SELECT LAEQ FROM ${getRoadsLevelsTableName(posSol)} RLS WHERE RL.IDRECEIVER = RLS.IDRECEIVER AND RL.PERIOD = RLS.PERIOD) / 10)) WHERE IDRECEIVER IN (SELECT IDRECEIVER FROM ${getRoadsLevelsTableName(posSol)});
            INSERT INTO RECEIVERS_LEVEL_$uueid SELECT THE_GEOM, IDRECEIVER, PERIOD, LAEQ FROM ${getRoadsLevelsTableName(posSol)} WHERE IDRECEIVER NOT IN (SELECT IDRECEIVER FROM RECEIVERS_LEVEL_$uueid);
            SELECT COUNT(*) FROM RECEIVERS_LEVEL_$uueid;
        """ as String

    }

    runScript(h2Connection, mergeLevelsQuery)

}

/**
 * Generate Buildings and Delaunay receivers
 * @param input User input data
 * @param pgConnection PostgreSQL database connection
 * @param uueid Infrastructure identifier
 * @param extractionEnvelopeGeometry Extraction envelope geometry
 * @param h2Connection H2 database connection
 * @param deltaBuildingsReceivers Delta for building receivers
 * @param mainConfiguration Main configuration map from config table on PostGIS database
 * @param stepsProgress Progress visitor for tracking steps
 * @return
 */
def generateReceivers(Map input,Connection pgConnection,String uueid, Geometry extractionEnvelopeGeometry, Connection h2Connection,double deltaBuildingsReceivers, Map mainConfiguration, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    ProgressVisitor subSteps = stepsProgress.subProcess(4)

    Sql h2Sql = new Sql(h2Connection)
    logger.info("Generate receivers on buildings")

    // Create a building table with KEEP field associated with with buildings with population or linked with erps
    // we will compute all the buildings, but remove receivers afterwards if the building is not erps or hold population
    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", """
            DROP TABLE IF EXISTS BUILDINGS_FOR_EXPOSURE;
            -- Filter buildings and force 2D as there is accum and union of geometries in Building_Grid script, we can not mix 2D and 3D polygons
            CREATE TABLE BUILDINGS_FOR_EXPOSURE(PK INTEGER NOT NULL PRIMARY KEY, THE_GEOM GEOMETRY, POP FLOAT, HEIGHT FLOAT, KEEP BOOLEAN) AS SELECT PK, ST_Force2D(THE_GEOM) AS THE_GEOM, POP, 10, (nb_logts_c > 0 OR erps_nature IS NOT NULL) KEEP FROM BUILDINGS;
        """ as String, "outputFormat", "json"),
            new EmptyProgressVisitor())
    def buildingsCountWithPop = h2Sql.firstRow("SELECT COUNT(*) CPT FROM BUILDINGS_FOR_EXPOSURE WHERE KEEP")[0] as Integer
    if(buildingsCountWithPop == 0) {
        logger.warn("No buildings with population found")
        h2Sql.execute("""
            CREATE TABLE RECEIVERS_BUILDINGS(PK INTEGER NOT NULL PRIMARY KEY, THE_GEOM GEOMETRY, POP FLOAT, BUILD_PK INTEGER);
        """)
    } else {
        logger.info(ScriptUtilities.execScript(new Building_Grid(), h2Connection,
                [tableBuilding: "BUILDINGS_FOR_EXPOSURE", delta: deltaBuildingsReceivers, height: 4.1, distance : 0.1],
                subSteps) as String)

        int numberOfBuildingsWithoutReceivers = h2Sql.firstRow("SELECT COUNT(*) FROM BUILDINGS_FOR_EXPOSURE WHERE KEEP AND PK NOT IN (SELECT DISTINCT BUILD_PK FROM RECEIVERS)")[0] as Integer
        if(numberOfBuildingsWithoutReceivers > 0) {
            logger.info("{} building${numberOfBuildingsWithoutReceivers > 1 ? 's' : ''} with population or erps does not have receivers", numberOfBuildingsWithoutReceivers)
            h2Sql
                    .rows("SELECT ST_Centroid(B.the_geom) the_geom FROM BUILDINGS_FOR_EXPOSURE B WHERE KEEP AND PK NOT IN (SELECT DISTINCT BUILD_PK FROM RECEIVERS) LIMIT 5")
                    .each {
                        logger.info("Building without receiver: {}", it.the_geom)
                    }
        }

        new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", """
            -- Remove receivers not associated with a building not concerned by exposure computation
            DELETE FROM RECEIVERS WHERE build_pk NOT IN (SELECT B.PK FROM BUILDINGS_FOR_EXPOSURE B WHERE B.KEEP = TRUE);
            DROP TABLE BUILDINGS_FOR_EXPOSURE;
            DROP TABLE IF EXISTS RECEIVERS_BUILDINGS;
            -- Rename receivers of buildings
            ALTER TABLE RECEIVERS RENAME TO RECEIVERS_BUILDINGS;            
        """),
            new EmptyProgressVisitor())
    }

    logger.info(ScriptUtilities.formatSqlQueryResult(h2Sql, "SELECT MIN(NBRECEIVERS) MIN_RECEIVERS, AVG(NBRECEIVERS) AVG_RECEIVERS, MAX(NBRECEIVERS) MAX_RECEIVERS, SUM(NBRECEIVERS) ALL_RECEIVERS FROM (SELECT build_pk, COUNT(PK) NBRECEIVERS FROM RECEIVERS_BUILDINGS GROUP BY build_pk)", 120))

    logger.info("Generate Delaunay receivers")
    // Fetch all roads using the UUEID query
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName}
        WHERE uueid = '${uueid}'"""
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
    }

    ScriptUtilities.execScript(new Add_Primary_Key(), h2Connection, [tableName: "ROADS", pkName: "PK"], subSteps)


    runScript(h2Connection, "DROP TABLE IF EXISTS TRIANGLES, RECEIVERS_DELAUNAY;");

    ScriptUtilities.execScript(new Delaunay_Grid(), h2Connection, [
            fence: extractionEnvelopeGeometry, tableBuilding: "BUILDINGS", sourcesTableName: "ROADS", maxCellDist: 1200,
            skipCellNoSourcesMinimalDistance : 2 * (mainConfiguration.confmaxsrcdist as Double),
            maxArea : 500, height: 4.1, outputTableName: "RECEIVERS_DELAUNAY", isoSurfaceInBuildings : true, exportTrianglesGeometries : true], subSteps)

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, "TRIANGLES", "THE_GEOM");

    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", """
            -- Create index
            CREATE INDEX ON TRIANGLES(PK_1);
            CREATE INDEX ON TRIANGLES(PK_2);
            CREATE INDEX ON TRIANGLES(PK_3);
            -- Copy triangles that are over buildings into another table
            -- It will be used later to fill areas under buildings with the same noise level
            DROP TABLE IF EXISTS TRIANGLES_OVER_BUILDINGS;
            CREATE TABLE TRIANGLES_OVER_BUILDINGS(PK INTEGER NOT NULL PRIMARY KEY, PK_1 INTEGER, PK_2 INTEGER,
             PK_3 INTEGER, THE_GEOM ${metaData.getSQL()}, build_pk INTEGER) AS SELECT T.PK, T.PK_1, T.PK_2, T.PK_3, T.THE_GEOM, MIN(B.PK) PKBAT
              FROM TRIANGLES T, BUILDINGS B
              WHERE T.THE_GEOM && B.THE_GEOM AND ST_Intersects(B.THE_GEOM, T.THE_GEOM)
              GROUP BY T.PK, T.PK_1, T.PK_2, T.PK_3, T.THE_GEOM;
            CREATE INDEX ON TRIANGLES_OVER_BUILDINGS (build_pk);
            -- Remove triangles that are over buildings
            DELETE FROM TRIANGLES WHERE PK IN (SELECT PK FROM TRIANGLES_OVER_BUILDINGS);
            -- Remove points not referenced by triangles
            DELETE FROM RECEIVERS_DELAUNAY R 
            WHERE NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_1 = R.PK)
              AND NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_2 = R.PK)
              AND NOT EXISTS (SELECT 1 FROM TRIANGLES T WHERE T.PK_3 = R.PK);
            -- Push the new receivers to the global receivers table
            DROP TABLE IF EXISTS RECEIVERS;
            SET @LASTDELAUNAY=(SELECT MAX(PK) FROM RECEIVERS_DELAUNAY);
            CREATE TABLE RECEIVERS AS SELECT PK, THE_GEOM FROM RECEIVERS_DELAUNAY;
            INSERT INTO RECEIVERS(PK, THE_GEOM) SELECT PK+@LASTDELAUNAY, THE_GEOM FROM RECEIVERS_BUILDINGS;
            CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM);
            ALTER TABLE RECEIVERS ALTER COLUMN PK INTEGER NOT NULL;
            ALTER TABLE RECEIVERS ADD PRIMARY KEY (PK);
        """ as String, "outputFormat", "html"),
            new EmptyProgressVisitor())
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

def enrichDem(Map input, String uueid, Connection h2Connection, Connection pgConnection, ProgressVisitor stepsProgress, String posSol) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Adapting digital elevation model..")
    ProgressVisitor demProgress = stepsProgress.subProcess(2)

    // Fetch road table with altitude using the UUEID query
    def roadQuery = """SELECT geom as the_geom, largeur as width
        FROM cbs_uge_input.n_routier_troncon_l_${input.projectionName}
        WHERE uueid = '${uueid}' and pos_sol = '$posSol' and (franchisst is null or franchisst = 'Pont')"""
    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "ROADS", true, batchSize)
        demProgress.endStep()
    }

    // Create a new DEM with road platforms
    def srid = Generate_sources.getSRIDFromTableExtensionName()[input.projectionName]
    ScriptUtilities.execScript(new Enrich_DEM_with_road(), h2Connection, [inputDEM: "DEM", inputRoad: "ROADS", roadWidth : "WIDTH", outputSuffix: "ENRICHED", inputSRID: srid], demProgress)
}

def runSimulation(Map mainConfiguration, Connection h2Connection, String posSol, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    ScriptUtilities.execScript(new Noise_level_from_source(),
            h2Connection, [
            tableBuilding: "BUILDINGS",
            tableSources: "LW_ROADS",
            tableReceivers: "RECEIVERS_FILTERED",
            tableDEM: "DEM_ENRICHED",
            tableGroundAbs: "LANDCOVER",
            tablePeriodAtmosphericSettings: "ATMOSPHERIC_SETTINGS",
            confReflOrder: mainConfiguration.confreflorder,
            confMaxSrcDist: mainConfiguration.confmaxsrcdist,
            confMaxReflDist: mainConfiguration.confmaxrefldist,
            confDiffVertical: mainConfiguration.confdiffvertical,
            confDiffHorizontal: mainConfiguration.confdiffhorizontal,
            confMinWallReflDist: 0.2 // ignore reflection distance, buildings receiver are at 0.1 m from facades
            ],
            stepsProgress)
    // Rename output table
    def outputTableName = getRoadsLevelsTableName(posSol)
    Sql h2Sql = new Sql(h2Connection)
    h2Sql.execute("""
        DROP TABLE IF EXISTS $outputTableName;
        ALTER TABLE RECEIVERS_LEVEL RENAME TO $outputTableName;
        CREATE INDEX ON $outputTableName ("IDRECEIVER", "PERIOD");
    """ as String)
}

def processLandCover(Map input,Connection pgConnection, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch land cover..")
    def landCoverQuery = """SELECT geom as the_geom, idnatsol as pk, natsol_lib as clc_lib, natsol_cno as g 
        FROM cbs_uge_input.c_naturesol_${input.projectionName} 
        WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry) AND NATSOL_CNO > 0"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(landCoverQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "LANDCOVER", true, batchSize)
    }

    // Apply projection to the table (if issues with copy or table is empty)
    def srid = Generate_sources.getSRIDFromTableExtensionName()[input.projectionName]
    new Sql(h2Connection).execute("CALL UpdateGeometrySRID('LANDCOVER', 'THE_GEOM', $srid)")
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
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "ATMOSPHERIC", true, batchSize)
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

def fetchRoads(Map input, Connection pgConnection, String uueid, Connection h2Connection, ProgressVisitor stepsProgress, String posSol, double maxSourceDistance) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch roads..")
    Sql h2Sql = new Sql(h2Connection)
    def roadsQuery = """SELECT * FROM cbs_uge_output.routier_emission_${input.projectionName} WHERE uueid = '$uueid' AND pos_sol='$posSol' AND (franchisst IS NULL OR franchisst = 'Pont')"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(roadsQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "LW_ROADS", true, batchSize)
    }

    if(JDBCUtilities.getRowCount(h2Connection, "LW_ROADS") == 0) {
        logger.info("No roads found for pos_sol = {}", posSol)
        return false
    }

    // Filter receivers only reachable from the roads geometries

    GeometryMetaData metaData =
            GeometryTableUtilities.getMetaData(h2Connection, "RECEIVERS", "THE_GEOM");

    logger.info("Receivers before filtering {}", h2Sql.firstRow("SELECT COUNT(*) FROM RECEIVERS")[0])

    runScript(h2Connection, """
        CREATE SPATIAL INDEX ON LW_ROADS(THE_GEOM);
        DROP TABLE IF EXISTS RECEIVERS_FILTERED;
        CREATE TABLE RECEIVERS_FILTERED(pk int not null primary key, the_geom ${metaData.getSQL()}) AS SELECT PK, THE_GEOM from RECEIVERS R 
            WHERE exists (select 1 from LW_ROADS LW where LW.THE_GEOM && st_expand(R.THE_geom, $maxSourceDistance) AND ST_Distance(LW.THE_GEOM, R.THE_GEOM) <= $maxSourceDistance limit 1);
    """)
    int receiverCount = h2Sql.firstRow("SELECT COUNT(*) FROM RECEIVERS_FILTERED")[0] as Integer
    logger.info("Receivers after filtering {}", receiverCount)
    return receiverCount > 0
}

def fetchBuildings(Map input, Connection pgConnection, String extractionEnvelopeGeometry, Connection h2Connection, ProgressVisitor stepsProgress, double wallAlpha) {
    Logger logger = LoggerFactory.getLogger(this.class)
    logger.info("Fetch buildings..")
    def projectionName = input.projectionName
    def tableQuery = """SELECT b.geom3d as the_geom, b.bat_haut as height, b.idbat, COALESCE(p.pop_bat, 0) as pop, COALESCE(b.nb_logts_c, 0) as nb_logts_c, b.bat_idtopo, bs.erps_nature
             FROM cbs_uge_input.c_batiment_s_${projectionName} b 
                LEFT JOIN cbs_uge_input.c_population_${projectionName} p ON b.idbat = p.idbat  
                LEFT JOIN cbs_uge_input.c_correspond_batiment_batimentsensible_${projectionName} s ON b.idbat = s.idbat
                LEFT JOIN cbs_uge_input.c_batimentsensible_${projectionName} bs ON s.iderps = bs.iderps
             WHERE ST_Intersects(b.geom3d, '${extractionEnvelopeGeometry}'::geometry)"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(tableQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "BUILDINGS", true, batchSize)
    }
    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", """
                CREATE SPATIAL INDEX ON BUILDINGS (THE_GEOM);
                ALTER TABLE buildings ADD COLUMN g float DEFAULT $wallAlpha;
                """ as String, "outputFormat", "json"),
            new EmptyProgressVisitor())

    def noiseBarrierQuery = """SELECT ST_Force3DZ(ST_CollectionHomogenize(geom)) as the_geom, hauteur as height, propriete, materiau1, idprotacou FROM cbs_uge_input.n_routier_protection_acoustique_hexa AS nrpah WHERE ST_Intersects(geom, '${extractionEnvelopeGeometry}'::geometry)"""

    try( Statement st = pgConnection.createStatement() ;
         ResultSet rs = st.executeQuery(noiseBarrierQuery)) {
        PostGISUtilities.copyResultSetToDatabase(pgConnection, rs, h2Connection, "BUILDINGS_BARRIERS", true, batchSize)
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

    new Execute_Query().exec(h2Connection,
            Map.of("sqlQueries", insertBarriersSql, "outputFormat", "json"),
            new EmptyProgressVisitor())

    ScriptUtilities.execScript(new Add_Primary_Key(), h2Connection, [tableName: "BUILDINGS", pkName: "PK"], stepsProgress)
}

static Map getDeptCodeFromExt() {
    return  ["hexa": '000', "guad": '971', "guya": '973', "mart": '972', "reun": '974' ]
}

@CompileStatic
static void runScript(Connection connection, String query, ProgressVisitor progressVisitor = new EmptyProgressVisitor()) {
    Logger logger = LoggerFactory.getLogger(Thread.currentThread().getName())
    logger.info(ScriptUtilities.execScript(new Execute_Query(),connection, [sqlQueries: query, "outputFormat" : "JSON"], progressVisitor) as String)
}