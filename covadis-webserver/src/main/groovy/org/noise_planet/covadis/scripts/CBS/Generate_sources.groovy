package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.spatial.edit.ST_UpdateZ
import org.h2gis.utilities.*
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.noise_planet.noisemodelling.jdbc.EmissionTableGenerator
import org.noise_planet.noisemodelling.pathfinder.utils.AcousticIndicatorsFunctions
import org.noise_planet.noisemodelling.webserver.utilities.Logging
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.stream.Collectors

title = 'Create sources table in PostGIS database'
description = 'Create sources table in PostGIS database'

inputs = [
        projectionName: [
                description: "Projection name",
                title: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger(this.class)

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
        pgConnection.setAutoCommit(true)
        Sql sql = new Sql(pgConnection)
        logger.info("Create merged traffic table")
        // Generate Traffic table that will be used as an input
        def projectionName = input.projectionName as String
        def trafficTableName = createMergeTrafficTable(projectionName, sql)

        // Create EMISSION TABLE
        def lwTableName = "cbs_uge_output.routier_emission_$projectionName"
        createLWRoads(pgConnection, [tableRoads : trafficTableName, outputTable: lwTableName], progress)

        // Post work on the lwTableName
        sql.execute("ALTER TABLE $lwTableName OWNER TO cbs_uge_group;" as String)
        // Fetch other fields using the primary key
        sql.execute("ALTER TABLE $lwTableName add column uueid varchar(20) NOT NULL;" as String)
        sql.execute("UPDATE $lwTableName SET uueid = (SELECT uueid FROM $trafficTableName tf WHERE pk = tf.pk);" as String)
        sql.execute("ALTER TABLE $lwTableName add column pos_sol varchar(20) NOT NULL;" as String)
        sql.execute("UPDATE $lwTableName SET pos_sol = (SELECT pos_sol FROM $trafficTableName tf WHERE pk = tf.pk);" as String)

        // Return results
        return Logging.formatSqlQueryResult(sql, "SELECT * FROM $lwTableName LIMIT 10" as String, 120)
    }
}

def createMergeTrafficTable(String projectionName, Sql sql){
    Logger logger = LoggerFactory.getLogger(this.class)

    def trafficOutputTableName = "cbs_uge_output.routier_trafic_$projectionName"
    Object projectionNameToProjectSRID = getSRIDFromTableExtensionName()

    def mergeTrafficSql = """
        DROP TABLE IF EXISTS $trafficOutputTableName;
        CREATE TABLE $trafficOutputTableName
(the_geom public.geometry(LINESTRINGZ, ${projectionNameToProjectSRID[projectionName]}) NULL,
id_troncon varchar NOT NULL,
id_route varchar(50) NULL,
lv_d int4 NULL,
lv_e int4 NULL,
lv_n int4 NULL,
mv_d float8 NULL,
mv_e float8 NULL,
mv_n float8 NULL,
hgv_d float8 NULL,
hgv_e float8 NULL,
hgv_n float8 NULL,
wav_d float8 NULL,
wav_e float8 NULL,
wav_n float8 NULL,
wbv_d float8 NULL,
wbv_e float8 NULL,
wbv_n float8 NULL,
lv_spd_d numeric NULL,
lv_spd_e numeric NULL,
lv_spd_n numeric NULL,
mv_spd_d numeric NULL,
mv_spd_e numeric NULL,
mv_spd_n numeric NULL,
hgv_spd_d numeric NULL,
hgv_spd_e numeric NULL,
hgv_spd_n numeric NULL,
wav_spd_d numeric(11) NULL,
wav_spd_e numeric(11) NULL,
wav_spd_n numeric(11) NULL,
wbv_spd_d numeric(11) NULL,
wbv_spd_e numeric(11) NULL,
wbv_spd_n numeric(11) NULL,
slope float8 NULL,
pvmt text NULL,
way text NULL,
uueid varchar(20) NULL,
pos_sol varchar(20) NULL,
temp_d numeric(11) NULL,
temp_n numeric(11) NULL,
temp_e numeric(11) NULL
);
INSERT INTO $trafficOutputTableName SELECT geom as THE_GEOM,
        a.idtroncon as ID_TRONCON,
        a.idroute as ID_ROUTE,
        b.tmhvld as LV_D,
        b.tmhvls as LV_E,
        b.tmhvln as LV_N,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b.pcentmpl/b.pcentpl ELSE 0 END) as MV_D,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b.pcentmpl/b.pcentpl ELSE 0 END) as MV_E,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b.pcentmpl/b.pcentpl ELSE 0 END) as MV_N,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b.pcenthpl/b.pcentpl ELSE 0 END) as HGV_D,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b.pcenthpl/b.pcentpl ELSE 0 END) as HGV_E,
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b.pcenthpl/b.pcentpl ELSE 0 END) as HGV_N,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rd * b.pcent2r4a/b.pcent2r ELSE 0 END) as WAV_D,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rs * b.pcent2r4a/b.pcent2r ELSE 0 END) as WAV_E,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rn * b.pcent2r4a/b.pcent2r ELSE 0 END) as WAV_N,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rd * b.pcent2r4b/b.pcent2r ELSE 0 END) as WBV_D,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rs * b.pcent2r4b/b.pcent2r ELSE 0 END) as WBV_E,
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rn * b.pcent2r4b/b.pcent2r ELSE 0 END) as WBV_N,
         c.vitessevl as LV_SPD_D, c.vitessevl as LV_SPD_E, c.vitessevl as LV_SPD_N,
         c.vitessepl as MV_SPD_D,c.vitessepl as MV_SPD_E, c.vitessepl as MV_SPD_N,
         c.vitessepl as HGV_SPD_D, c.vitessepl as HGV_SPD_E, c.vitessepl as HGV_SPD_N,
         c.vitesse4a as WAV_SPD_D, c.vitesse4a as WAV_SPD_E, c.vitesse4a as WAV_SPD_N,
         c.vitesse4b as WBV_SPD_D, c.vitesse4b as WBV_SPD_E, c.vitesse4b as WBV_SPD_N,
         ROUND((a.zfin-a.zdeb)/ ST_LENGTH(a.geom)*100) as SLOPE,
         'FR_R2' as PVMT,
         (CASE  WHEN a.sens = '01' THEN '01'
           WHEN a.sens = '02' THEN '02'
           ELSE '03'
          END) as WAY,
         a.uueid as UUEID,
         a.pos_sol AS POS_SOL,
         d.temp_6_18 as TEMP_D,
         d.temp_22_6 as TEMP_N,
         d.temp_18_22 as TEMP_E
        FROM
         cbs_uge_input.n_routier_troncon_l_${projectionName} a
         CROSS JOIN LATERAL (
            SELECT 
                s.temp_6_18, 
                s.temp_22_6, 
                s.temp_18_22
            FROM 
                cbs_uge_input.nm_stations_${projectionName} s
            ORDER BY 
                s.the_geom <-> a.geom
            LIMIT 1
        ) AS d,
         cbs_uge_input.n_routier_trafic_${projectionName} b,
         cbs_uge_input.n_routier_vitesse_${projectionName} c
        WHERE
         ST_LENGTH(geom) > 0 and
         a.idtroncon=b.idtroncon and
         b.idtroncon=c.idtroncon and
         b.tmhvld >= 0 AND b.tmhvls >= 0 AND b.tmhvln >= 0 AND
         (b.pcentpl = 0 OR b.pcentpl >= b.pcentmpl) AND
         (b.pcent2r = 0 OR b.pcent2r >= b.pcent2r4a) AND
         (b.pcent2r = 0 OR b.pcent2r >= b.pcent2r4b) AND
         a.uueid IS NOT NULL;
        ALTER TABLE $trafficOutputTableName ADD COLUMN PK INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY;
        ALTER TABLE $trafficOutputTableName ALTER COLUMN ID_TRONCON SET NOT NULL;
        CREATE UNIQUE INDEX ON $trafficOutputTableName (ID_TRONCON);
        CREATE INDEX ON $trafficOutputTableName USING GIST(THE_GEOM);
        ALTER TABLE $trafficOutputTableName OWNER TO cbs_uge_group;

        COMMENT ON COLUMN ${trafficOutputTableName}.id_troncon IS 'Identifiant unique du tronçon (PK)';
        COMMENT ON COLUMN ${trafficOutputTableName}.id_route IS 'Identifiant de la route parente';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_d IS 'Hourly average light vehicle count (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_e IS 'Hourly average light vehicle count (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_n IS 'Hourly average light vehicle count (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_d IS 'Hourly average medium heavy vehicles, delivery vans > 3.5 tons, buses, etc. (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_e IS 'Hourly average medium heavy vehicles, delivery vans > 3.5 tons, buses, etc. (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_n IS 'Hourly average medium heavy vehicles, delivery vans > 3.5 tons, buses, etc. (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_d IS 'Hourly average heavy duty vehicles, touring cars, buses, with 3+ axles (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_e IS 'Hourly average heavy duty vehicles, touring cars, buses, with 3+ axles (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_n IS 'Hourly average heavy duty vehicles, touring cars, buses, with 3+ axles (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_d IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc count (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_e IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc count (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_n IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc count (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_d IS 'Hourly average motorcycles, tricycles or quads > 50 cc count (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_e IS 'Hourly average motorcycles, tricycles or quads > 50 cc count (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_n IS 'Hourly average motorcycles, tricycles or quads > 50 cc count (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_spd_d IS 'Hourly average light vehicle speed (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_spd_e IS 'Hourly average light vehicle speed (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.lv_spd_n IS 'Hourly average light vehicle speed (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_spd_d IS 'Hourly average medium heavy vehicles speed (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_spd_e IS 'Hourly average medium heavy vehicles speed (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.mv_spd_n IS 'Hourly average medium heavy vehicles speed (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_spd_d IS 'Hourly average heavy duty vehicles speed (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_spd_e IS 'Hourly average heavy duty vehicles speed (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.hgv_spd_n IS 'Hourly average heavy duty vehicles speed (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_spd_d IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc speed (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_spd_e IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc speed (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wav_spd_n IS 'Hourly average mopeds, tricycles or quads ≤ 50 cc speed (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_spd_d IS 'Hourly average motorcycles, tricycles or quads > 50 cc speed (6-18h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_spd_e IS 'Hourly average motorcycles, tricycles or quads > 50 cc speed (18-22h)';
        COMMENT ON COLUMN ${trafficOutputTableName}.wbv_spd_n IS 'Hourly average motorcycles, tricycles or quads > 50 cc speed (22-6h)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.slope IS 'Slope (in %) of the road section';
        COMMENT ON COLUMN ${trafficOutputTableName}.pvmt IS 'CNOSSOS road pavement identifier (ex: NL05)(default NL08)';
        COMMENT ON COLUMN ${trafficOutputTableName}.way IS 'Traffic flow: 1=One way (with slope), 2=One way (inverse slope), 3=Bi-directional';
        COMMENT ON COLUMN ${trafficOutputTableName}.pos_sol IS 'Position index relative to the ground (ex: -5 to +5)';
        
        COMMENT ON COLUMN ${trafficOutputTableName}.temp_d IS 'Temperature Day (6-18h) from nearest station';
        COMMENT ON COLUMN ${trafficOutputTableName}.temp_e IS 'Temperature Evening (18-22h) from nearest station';
        COMMENT ON COLUMN ${trafficOutputTableName}.temp_n IS 'Temperature Night (22-6h) from nearest station';
        """ as String

    // Run query on external database
    logger.info("Execute {}", mergeTrafficSql)
    sql.execute(mergeTrafficSql)
    logger.info("Inserted $sql.updateCount rows in $trafficOutputTableName")
    return trafficOutputTableName
}

static Map getSRIDFromTableExtensionName() {
    return  ["hexa": 2154, "guad": 5490, "guya": 2972, "mart": 5490, "reun": 2975]
}

@CompileStatic
def createLWRoads(Connection connection, Map input, ProgressVisitor progress) {

    int coefficientVersion =  input.getOrDefault("coefficientVersion",2) as Integer


    DBTypes dbType = DBUtils.getDBType(connection)

    def outputTableName = TableLocation.capsIdentifier(input.getOrDefault("outputTable", "lw_roads") as String, dbType)

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    if(!connection.isWrapperFor(ConnectionWrapper.class)) {
        connection = new ConnectionWrapper(connection)
    }

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Road Emission from DEN')
    logger.info("inputs {}", input) // log inputs of the run


    // -------------------
    // Get every inputs
    // -------------------

    String sources_table_name = input['tableRoads']
    TableLocation sourceTableIdentifier = TableLocation.parse(sources_table_name, dbType)

    // do it case-insensitive
    sources_table_name = sources_table_name.toUpperCase()

    //Get optional geometry field of the source table
    List<String> geomFields = GeometryTableUtilities.getGeometryColumnNames(connection, sourceTableIdentifier)

    //Get the primary key field of the source table
    Tuple<String, Integer> primaryKeyColumn = JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(connection, TableLocation.parse( sources_table_name, dbType))

    // -------------------
    // Init table LW_ROADS
    // -------------------

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    def lowerCaseColumnNames = JDBCUtilities.getColumnNames(connection, sourceTableIdentifier).stream()
            .map { it.toLowerCase() }
            .collect(Collectors.toList())

    // If there is a period field, it means that we will not found the D E N fields before traffic fields names
    boolean hasPeriodField = lowerCaseColumnNames.contains("period")
    boolean hasIdSourceField = lowerCaseColumnNames.contains("idsource")

    // drop table LW_ROADS if exists and the create and prepare the table
    sql.execute("drop table if exists $outputTableName;" as String)

    // Use lists to collect the column definitions and column names
    def createDefinitions = []
    def columnNames = []

    if (primaryKeyColumn != null) {
        def pkName = primaryKeyColumn.first()
        createDefinitions << "${pkName} integer not null"
        columnNames << pkName
    }

    if (hasIdSourceField) {
        createDefinitions << "IDSOURCE integer"
        columnNames << "IDSOURCE"
    }

    if (geomFields.size() > 0) {
        def geomName = geomFields.get(0)
        columnNames << geomName

        def tupMeta = GeometryTableUtilities.getFirstColumnMetaData(connection, sourceTableIdentifier)
        if (tupMeta != null) {
            tupMeta.second().setHasZ(true)
            createDefinitions << "$geomName ${tupMeta.second().SQL}"
            logger.warn("The geometry field ${geomName} z value will be forced to 0.05m height.")
        }
    }

    if (!hasPeriodField) {
        ["D", "E", "N"].each { period ->
            ["63", "125", "250", "500", "1000", "2000", "4000", "8000"].each { freq ->
                def col = "HZ${period}${freq}"
                createDefinitions << "${col} double precision"
                columnNames << col
            }
        }
    } else {
        createDefinitions << "PERIOD varchar"
        columnNames << "PERIOD"

        ["63", "125", "250", "500", "1000", "2000", "4000", "8000"].each { freq ->
            def col = "HZ${freq}"
            createDefinitions << "${col} double precision"
            columnNames << col
        }
    }

    // 1. Create the Table Query
    // join() adds commas only between elements
    def createTableQuery = "CREATE TABLE $outputTableName (${createDefinitions.join(", ")});"
    sql.execute(createTableQuery as String)

    // 2. Prepared Insert Query
    int fieldCount = columnNames.size()
    // Create a list of '?' characters equal to the number of fields
    def placeholders = (["?"] * fieldCount).join(", ")

    def qry = "INSERT INTO $outputTableName (" + columnNames.join(", ") + ") VALUES (" + placeholders + ");"

    // --------------------------------------
    // Start calculation and fill the table
    // --------------------------------------

    // Get size of the table (number of road segments
    PreparedStatement st = connection.prepareStatement("SELECT COUNT(*) AS total FROM " + sources_table_name)
    ResultSet rs1 = st.executeQuery().unwrap(ResultSet.class)
    int nbRoads = 0
    while (rs1.next()) {
        nbRoads = rs1.getInt("total")
        logger.info('The table '+sources_table_name+' has ' + nbRoads + ' lines.')
    }
    ProgressVisitor subProgress = progress.subProcess(nbRoads)

    sql.withBatch(100, qry) { ps ->
        st = connection.prepareStatement("SELECT * FROM " + sources_table_name)
        st.setFetchSize(500);
        st.setFetchDirection(ResultSet.FETCH_FORWARD)
        SpatialResultSet rs = st.executeQuery().unwrap(SpatialResultSet.class)

        Map<String, Integer> sourceFieldsCache = new HashMap<>()
        while (rs.next() && !progress.isCanceled()) {
            List<Object> parameters = new ArrayList<>()
            if(primaryKeyColumn != null) {
                parameters.add(rs.getInt(primaryKeyColumn.first()))
            }
            if(hasIdSourceField) {
                parameters.add(rs.getInt("IDSOURCE"))
            }
            if(geomFields.size() > 0) {
                parameters.add(ST_UpdateZ.updateZ(rs.getGeometry(geomFields.get(0)), 0.05d))
            }
            if(hasPeriodField) {
                parameters.add(rs.getString("PERIOD"))
                // Slope value will be overwritten if the slope field is present
                double slope = EmissionTableGenerator.getSlope(rs)
                double[] emissionValues = EmissionTableGenerator.getEmissionFromTrafficTable(rs, "", slope, coefficientVersion, sourceFieldsCache)
                for(double val : emissionValues) {
                    parameters.add(val)
                }
            } else {
                double[][] results = EmissionTableGenerator.computeLw(rs, coefficientVersion, sourceFieldsCache)
                def lday = AcousticIndicatorsFunctions.wToDb(results[0])
                def levening = AcousticIndicatorsFunctions.wToDb(results[1])
                def lnight = AcousticIndicatorsFunctions.wToDb(results[2])
                for(def val : lday) {
                    parameters.add(val)
                }
                for(def val : levening) {
                    parameters.add(val)
                }
                for(def val : lnight) {
                    parameters.add(val)
                }
            }
            ps.addBatch(parameters)
            subProgress.endStep()
        }
    }

    if(primaryKeyColumn != null) {
        // Set primary key to the road table
        sql.execute("ALTER TABLE $outputTableName ADD PRIMARY KEY (${primaryKeyColumn.first()});  " as String)
    }

    // Create spatial index
    if(geomFields.size() > 0) {
        logger.info("Create spatial index on the geometry field ${geomFields.get(0)}")
        JDBCUtilities.createSpatialIndex(connection, TableLocation.parse(outputTableName, dbType), geomFields.get(0))
    }

    resultString = "Calculation Done ! The table $outputTableName has been created."

    // print to command window
    logger.info('\nResult : ' + resultString)
    logger.info("End : $outputTableName from Emission")

    // print to WPS Builder
    return [result: outputTableName]
}
