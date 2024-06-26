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
    - Check if railway sound levels are realistic (they seem to be low)
    - Check if railway sound sources are well spaced with good heights
    - Add Metadatas
    - remove unnecessary lines (il y en a beaucoup)
    - add more logs for users
    - add GS field to LW_RAILWAY table
 */


package org.noise_planet.noisemodelling.wps.plamade

import org.h2gis.functions.spatial.convert.ST_Force3D
import org.h2gis.functions.spatial.edit.ST_UpdateZ
import groovy.sql.Sql
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SpatialResultSet
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.LineString
import org.noise_planet.noisemodelling.emission.LineSource
import org.noise_planet.noisemodelling.emission.railway.RailWayParameters
import org.noise_planet.noisemodelling.emission.railway.cnossos.RailwayCnossos
import org.noise_planet.noisemodelling.emission.utils.Utils
import org.noise_planet.noisemodelling.jdbc.LDENConfig
import org.noise_planet.noisemodelling.jdbc.LDENPropagationProcessData
import org.noise_planet.noisemodelling.jdbc.RailWayLWIterator
import org.noise_planet.noisemodelling.jdbc.utils.MakeLWTable
import org.noise_planet.noisemodelling.pathfinder.CnossosPropagationData
import org.noise_planet.noisemodelling.pathfinder.utils.PowerUtils
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

title = 'Compute LW_RAILWAY and LW_ROADS'
description = 'Compute LW_RAILWAY and LW_ROADS from traffic flow rate and speed estimates (specific format, see input details).' +
        '</br> Tables must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed.' +
        '</br> </br> <b> The output tables are called : LW_RAILWAY and LW_ROADS </b> ' +
        'and contain : </br>' +
        '- <b> IDRECEIVER  </b> : an identifier (INTEGER, PRIMARY KEY). </br>' +
        '- <b> THE_GEOM </b> : the 3D geometry of the receivers (POINT).</br> ' +
        '-  <b> Hz63, Hz125, Hz250, Hz500, Hz1000,Hz2000, Hz4000, Hz8000 </b> : columns giving the day emission sound level for each octave band or third octave band (FLOAT).'

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

def forgeCreateTable(Sql sql, String tableName, LDENConfig ldenConfig, String geomField, String tableReceiver, String tableResult) {
    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    StringBuilder sb = new StringBuilder("CREATE TABLE ");
    sb.append(tableName);
    if (!ldenConfig.mergeSources) {
        sb.append(" (IDRECEIVER bigint NOT NULL");
        sb.append(", IDSOURCE bigint NOT NULL");
    } else {
        sb.append(" (IDRECEIVER bigint NOT NULL");
    }
    sb.append(", THE_GEOM geometry")
    for (int idfreq = 0; idfreq < ldenConfig.propagationProcessPathData.freq_lvl.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(ldenConfig.propagationProcessPathData.freq_lvl.get(idfreq));
        sb.append(" numeric(5, 2)");
    }
    sb.append(", LAEQ numeric(5, 2), LEQ numeric(5, 2) ) AS SELECT PK");
    if (!ldenConfig.mergeSources) {
        sb.append(", IDSOURCE");
    }
    sb.append(", ")
    sb.append(geomField)
    for (int idfreq = 0; idfreq < ldenConfig.propagationProcessPathData.freq_lvl.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(ldenConfig.propagationProcessPathData.freq_lvl.get(idfreq));
    }
    sb.append(", LAEQ, LEQ FROM ")
    sb.append(tableReceiver)
    if (!ldenConfig.mergeSources) {
        // idsource can't be null so we can't left join
        sb.append(" a, ")
        sb.append(tableResult)
        sb.append(" b WHERE a.PK = b.IDRECEIVER")
    } else {
        sb.append(" a LEFT JOIN ")
        sb.append(tableResult)
        sb.append(" b ON a.PK = b.IDRECEIVER")
    }
    sql.execute(sb.toString())
    // apply pk
    logger.info("Add primary key on " + tableName)
    if (!ldenConfig.mergeSources) {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER, IDSOURCE)")
    } else {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER)")
    }
}

// main function of the script
def exec(Connection connection, input) {
    DBTypes dbTypes = DBUtils.getDBType(connection)

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)


    // Update metadata table with start time
    sql.execute(String.format("UPDATE metadata SET emi_conf =" + input.confId))
    sql.execute(String.format("UPDATE metadata SET emi_start = NOW();"))


    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Compute Emission Noise Levels')
    logger.info("inputs {}", input) // log inputs of the run

    // -------------------
    // Get every inputs
    // -------------------

    // Pointing the 'rail_sections' table
    String rail_sections = "rail_sections"
    // do it case-insensitive
    rail_sections = rail_sections.toUpperCase()


    // Pointing the 'rail_traffic' table
    String rail_traffic = "rail_traffic"
    // do it case-insensitive
    rail_traffic = rail_traffic.toUpperCase()

    //Get the geometry field of the source table
    TableLocation sourceTableIdentifier = TableLocation.parse(rail_sections)
    int nrows = JDBCUtilities.getRowCount(connection, rail_sections)

    if (nrows!=0) {

        // Check if srid are in metric projection.
        int sridSources = GeometryTableUtilities.getSRID(connection, TableLocation.parse(rail_sections))
        if (sridSources == 3785 || sridSources == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+rail_sections+".")
        if (sridSources == 0) throw new IllegalArgumentException("Error : The table "+rail_sections+" does not have an associated SRID.")


        //Get the primary key field of the source table
        int pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(rail_sections, dbTypes))
        if (pkIndex < 1) {
            sql.execute("ALTER TABLE " + rail_sections + " ADD PK INT AUTO_INCREMENT PRIMARY KEY;")
            // throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
        }


        //Get the primary key field of the source table
        pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection,  TableLocation.parse(rail_traffic, dbTypes))
        if (pkIndex < 1) {
            sql.execute("ALTER TABLE " + rail_traffic + " ADD PK INT AUTO_INCREMENT PRIMARY KEY;")
            //throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
        }

        // Pointing the 'receivers' table
        String receivers_table_name = "receivers"
        // do it case-insensitive
        receivers_table_name = receivers_table_name.toUpperCase()
        //Get the geometry field of the receiver table
        TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)
        List<String> geomFieldsRcv = GeometryTableUtilities.getGeometryColumnNames(connection, receiverTableIdentifier)
        if (geomFieldsRcv.isEmpty()) {
            throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier))
        }
        // Check if srid are in metric projection and are all the same.
        int sridReceivers = GeometryTableUtilities.getSRID(connection, TableLocation.parse(receivers_table_name))
        if (sridReceivers == 3785 || sridReceivers == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+receivers_table_name+".")
        if (sridReceivers == 0) throw new IllegalArgumentException("Error : The table "+receivers_table_name+" does not have an associated SRID.")
        if (sridReceivers != sridSources) throw new IllegalArgumentException("Error : The SRID of table "+rail_sections+" and "+receivers_table_name+" are not the same.")


        // Get the primary key field of the receiver table
        int pkIndexRecv = JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(receivers_table_name, dbTypes))
        if (pkIndexRecv < 1) {
            throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", receiverTableIdentifier))
        }

        // --------------------------------------------
        // Initialize NoiseModelling emission part
        // --------------------------------------------

        // ----------------------------------------------------------------------------
        // Prepare LW table

        MakeLWTable.makeTrainLWTable(connection, rail_sections, rail_traffic, "LW_RAILWAY")

        // Add plamade fields
        sql.execute("ALTER TABLE LW_RAILWAY ADD COLUMN ID_SECTION VARCHAR USING (SELECT IDSECTION FROM " +
                rail_sections + " RS WHERE RS.PK = LW_RAILWAY.PK_SECTION)")
        sql.execute("ALTER TABLE LW_RAILWAY ADD COLUMN UUEID VARCHAR USING (SELECT UUEID FROM " +
                rail_sections + " RS WHERE RS.PK = LW_RAILWAY.PK_SECTION)")

        //DELETE sections where LW are lower than 0
        //sql.execute("DELETE FROM LW_RAILWAY WHERE LEAST (LWD50, LWD63, LWD80, LWD100, LWD125, LWD160, LWD200, LWD250, LWD315, LWD400, LWD500, LWD630, LWD800, LWD1000, LWD1250, LWD1600, LWD2000, LWD2500, LWD3150, LWD4000, LWD5000, LWD6300, LWD8000, LWD10000, LWE50, LWE63, LWE80, LWE100, LWE125, LWE160, LWE200, LWE250, LWE315, LWE400, LWE500, LWE630, LWE800, LWE1000, LWE1250, LWE1600, LWE2000, LWE2500, LWE3150, LWE4000, LWE5000, LWE6300, LWE8000, LWE10000, LWN50, LWN63, LWN80, LWN100, LWN125, LWN160, LWN200, LWN250, LWN315, LWN400, LWN500, LWN630, LWN800, LWN1000, LWN1250, LWN1600, LWN2000, LWN2500, LWN3150, LWN4000, LWN5000, LWN6300, LWN8000, LWN10000)<0;")

        sql.execute("UPDATE LW_RAILWAY SET THE_GEOM = ST_SETSRID(THE_GEOM, "+sridSources+")")
        sql.execute("CREATE SPATIAL INDEX ON LW_RAILWAY(THE_GEOM);")

    }//End if (check if RAIL_SECTIONS exists or is empty)


    // -------------------
    // Init table LW_ROADS
    // -------------------

    // Drop table LW_ROADS if exists and the create and prepare the table
    sql.execute("DROP TABLE IF EXISTS LW_ROADS;")
    sql.execute("CREATE TABLE LW_ROADS (pk integer, the_geom Geometry, ID_TRONCON varchar, UUEID varchar, " +
            "LWD63 double precision, LWD125 double precision, LWD250 double precision, LWD500 double precision, LWD1000 double precision, LWD2000 double precision, LWD4000 double precision, LWD8000 double precision," +
            "LWE63 double precision, LWE125 double precision, LWE250 double precision, LWE500 double precision, LWE1000 double precision, LWE2000 double precision, LWE4000 double precision, LWE8000 double precision," +
            "LWN63 double precision, LWN125 double precision, LWN250 double precision, LWN500 double precision, LWN1000 double precision, LWN2000 double precision, LWN4000 double precision, LWN8000 double precision);")

    def qry = 'INSERT INTO LW_ROADS(pk,the_geom, ID_TRONCON, UUEID, ' +
            'LWD63, LWD125, LWD250, LWD500, LWD1000,LWD2000, LWD4000, LWD8000,' +
            'LWE63, LWE125, LWE250, LWE500, LWE1000,LWE2000, LWE4000, LWE8000,' +
            'LWN63, LWN125, LWN250, LWN500, LWN1000,LWN2000, LWN4000, LWN8000) ' +
            'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'


    // --------------------------------------
    // Start calculation and fill the table
    // --------------------------------------

    // Get Class to compute LW
    LDENConfig ldenConfig2 = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_TRAFFIC_FLOW)
    ldenConfig2.setCoefficientVersion(1)
    ldenConfig2.setComputeLDay(true)
    ldenConfig2.setComputeLEvening(true)
    ldenConfig2.setComputeLNight(true)
    ldenConfig2.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.TIME_PERIOD_DAY,
            new PropagationProcessPathData(false));

    LDENPropagationProcessData ldenData = new LDENPropagationProcessData(null, ldenConfig2)

    def sources_table_name = "ROADS"

        //Get the primary key field of the source table
    def pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(sources_table_name, dbTypes))
    if (pkIndex < 1) {
        throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
    }


    // Check if srid are in metric projection.
    int sridSources = GeometryTableUtilities.getSRID(connection, TableLocation.parse(sources_table_name))
    if (sridSources == 3785 || sridSources == 4326) throw new IllegalArgumentException("Error : Please use a metric projection for "+sources_table_name+".")
    if (sridSources == 0) throw new IllegalArgumentException("Error : The table "+sources_table_name+" does not have an associated SRID.")


    // Get size of the table (number of road segments)
    PreparedStatement st = connection.prepareStatement("SELECT COUNT(*) AS total FROM " + sources_table_name)
    ResultSet rs2 = st.executeQuery().unwrap(ResultSet.class)
    int nbRoads = 0
    while (rs2.next()) {
        nbRoads = rs2.getInt("total")
        logger.info('The table Roads has ' + nbRoads + ' road segments.')
    }


    int k = 0
    int currentVal = 0
    sql.withBatch(100, qry) { ps ->
        st = connection.prepareStatement("SELECT * FROM " + sources_table_name)
        SpatialResultSet rs = st.executeQuery().unwrap(SpatialResultSet.class)

        while (rs.next()) {
            k++
            Geometry geo = rs.getGeometry()

            // Compute emission sound level for each road segment
            def results = ldenData.computeLw(rs)
            def lday = PowerUtils.wToDba(results[0])
            def levening = PowerUtils.wToDba(results[1])
            def lnight = PowerUtils.wToDba(results[2])
            // fill the LW_ROADS table
            ps.addBatch(rs.getLong(pkIndex) as Integer, geo as Geometry, rs.getString(2) as String, rs.getString(45) as String,
                    lday[0] as Double, lday[1] as Double, lday[2] as Double,
                    lday[3] as Double, lday[4] as Double, lday[5] as Double,
                    lday[6] as Double, lday[7] as Double,
                    levening[0] as Double, levening[1] as Double, levening[2] as Double,
                    levening[3] as Double, levening[4] as Double, levening[5] as Double,
                    levening[6] as Double, levening[7] as Double,
                    lnight[0] as Double, lnight[1] as Double, lnight[2] as Double,
                    lnight[3] as Double, lnight[4] as Double, lnight[5] as Double,
                    lnight[6] as Double, lnight[7] as Double)
        }
    }

    // Add Z dimension to the road segments
    sql.execute("UPDATE LW_ROADS SET THE_GEOM = ST_UPDATEZ(The_geom,0.05);")

    // Add primary key to the road table
    sql.execute("ALTER TABLE LW_ROADS ALTER COLUMN PK INT NOT NULL;")
    sql.execute("ALTER TABLE LW_ROADS ADD PRIMARY KEY (PK);")    
    sql.execute("UPDATE LW_ROADS SET THE_GEOM = ST_SETSRID(THE_GEOM, "+sridSources+")")

    sql.execute("CREATE SPATIAL INDEX ON LW_ROADS(THE_GEOM);")
    

    sql.execute(String.format("UPDATE metadata SET emi_end = NOW();"))


    resultString = "Calculation Done! Compute Emission Noise Levels table(s) have been created."

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : LDAY from Traffic')

    // print to WPS Builder
    return resultString


}

