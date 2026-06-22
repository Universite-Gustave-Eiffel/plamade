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

        sql.execute("ALTER TABLE $lwTableName OWNER TO cbs_uge_group;")

        // Return results
        return Logging.formatSqlQueryResult(sql, "SELECT * FROM $lwTableName LIMIT 10" as String, 120)
    }
}

def createMergeTrafficTable(String projectionName, Sql sql){
    Logger logger = LoggerFactory.getLogger(this.class)

    def trafficOutputTableName = "cbs_uge_output.routier_trafic_$projectionName"
    def projectionNameToProjectSRID = ["hexa": 2154, "guad": 5490, "guya": 2972, "mart": 5490, "reun": 2975]
    def geometryField = "geom${projectionNameToProjectSRID[projectionName]}"

    def mergeTrafficSql = """
        DROP TABLE IF EXISTS $trafficOutputTableName;
        CREATE TABLE $trafficOutputTableName AS SELECT $geometryField as "THE_GEOM",
        a.idtroncon as "ID_TRONCON",
        a.idroute as "ID_ROUTE",
        b.tmhvld as "LV_D",
        b.tmhvls as "LV_E",
        b.tmhvln as "LV_N",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_D",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_E",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b.pcentmpl/b.pcentpl ELSE 0 END) as "MV_N",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpld * b.pcenthpl/b.pcentpl ELSE 0 END) as "HGV_D",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpls * b.pcenthpl/b.pcentpl ELSE 0 END) as "HGV_E",
         (CASE  WHEN b.pcentpl > 0 THEN b.tmhpln * b.pcenthpl/b.pcentpl ELSE 0 END) as "HGV_N",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rd * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_D",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rs * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_E",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rn * b.pcent2r4a/b.pcent2r ELSE 0 END) as "WAV_N",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rd * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_D",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rs * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_E",
         (CASE  WHEN b.pcent2r > 0 THEN b.tmh2rn * b.pcent2r4b/b.pcent2r ELSE 0 END) as "WBV_N",
         c.vitessevl as "LV_SPD_D", c.vitessevl as "LV_SPD_E", c.vitessevl as "LV_SPD_N",
         c.vitessepl as "MV_SPD_D",c.vitessepl as "MV_SPD_E", c.vitessepl as "MV_SPD_N",
         c.vitessepl as "HGV_SPD_D", c.vitessepl as "HGV_SPD_E", c.vitessepl as "HGV_SPD_N",
         c.vitesse4a as "WAV_SPD_D", c.vitesse4a as "WAV_SPD_E", c.vitesse4a as "WAV_SPD_N",
         c.vitesse4b as "WBV_SPD_D", c.vitesse4b as "WBV_SPD_E", c.vitesse4b as "WBV_SPD_N",
         ROUND((a.zfin-a.zdeb)/ ST_LENGTH(a.$geometryField)*100) as "SLOPE",
         'FR_R2' as "PVMT",
         (CASE  WHEN a.sens = '01' THEN '01'
           WHEN a.sens = '02' THEN '02'
           ELSE '03'
          END) as "WAY",
         a.uueid as "UUEID"
        FROM
         cbs_uge_input.n_routier_troncon_l_$projectionName a,
         cbs_uge_input.n_routier_trafic_$projectionName b,
         cbs_uge_input.n_routier_vitesse_$projectionName c
        WHERE
         ST_LENGTH($geometryField) > 0 and
         a.idtroncon=b.idtroncon and
         b.idtroncon=c.idtroncon and
         b.tmhvld >= 0 AND b.tmhvls >= 0 AND b.tmhvln >= 0 AND
         (b.pcentpl = 0 OR b.pcentpl >= b.pcentmpl) AND
         (b.pcent2r = 0 OR b.pcent2r >= b.pcent2r4a) AND
         (b.pcent2r = 0 OR b.pcent2r >= b.pcent2r4b);
        ALTER TABLE $trafficOutputTableName ALTER COLUMN "ID_TRONCON" SET NOT NULL;
        ALTER TABLE $trafficOutputTableName ADD PRIMARY KEY("ID_TRONCON");
        CREATE INDEX ON $trafficOutputTableName USING GIST("THE_GEOM");
        ALTER TABLE $trafficOutputTableName OWNER TO cbs_uge_group;
        """ as String

    // Run query on external database
    logger.info("Execute {}", mergeTrafficSql)
    sql.execute(mergeTrafficSql)
    logger.info("Inserted $sql.updateCount rows in $trafficOutputTableName")
    return trafficOutputTableName
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
