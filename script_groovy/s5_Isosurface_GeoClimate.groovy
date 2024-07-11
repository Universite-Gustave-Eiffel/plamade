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
 * @Author Samuel Marsault, Trainee
 */

/* TODO
    - Check spatial index and srids
    - Add Metadatas
    - remove unnecessary lines (il y en a beaucoup)
    - Change Name of the file
    - Change descriptions
    - remove temporary tables
    - Export shp files
 */



package org.noise_planet.noisemodelling.wps.plamade;

import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.jdbc.BezierContouring
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import groovy.sql.Sql

import org.h2gis.utilities.JDBCUtilities

title = 'Create an isosurface from a NoiseModelling result table and its associated TRIANGLES table.'

description = 'Generate one isosurface table per road or rail infrastructure, both for LDEN and LNIGHT.'

inputs = [
        rail_or_road: [
                name       : 'Rail or Road',
                title      : 'Rail or Road',
                description: 'Rail (1) ou Road (2)',
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

def exec(Connection connection, input) {

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    Sql sql = new Sql(connection)

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Compute Isosurfaces')
    logger.info("inputs {}", input) // log inputs of the run

    Integer railRoad = input["rail_or_road"]

    ProgressVisitor progressLogger

    if("progressVisitor" in input) {
        progressLogger = input["progressVisitor"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1);
    }
    /*
    //List of iso classes :  isoClasses

    // LDEN classes for A maps : 55-59, 60-64, 65-69, 70-74 et >75 dB
    def isoLevelsLDEN=[55.0d,60.0d,65.0d,70.0d,75.0d,200.0d]
    // LNIGHT classes for A maps : 50-54, 55-59, 60-64, 65-69 et >70 dB
    def isoLevelsLNIGHT=[50.0d,55.0d,60.0d,65.0d,70.0d,200.0d]

    // LDEN classes for C maps: >68 dB
    def isoCLevelsLDEN=[68.0d,200.0d]
    // LNIGHT classes for C maps : >62 dB
    def isoCLevelsLNIGHT=[62.0d,200.0d]

    // LDEN classes for C maps and : >73 dB
    def isoCFerConvLevelsLDEN=[73.0d,200.0d]
    // LNIGHT classes for C maps : >65 dB
    def isoCFerConvLevelsLNIGHT=[65.0d,200.0d]


    // List of input tables : inputTable
    String source, tableDEN, tableNIGHT
    if (railRoad==1){
        source = "RAIL_SECTIONS"
        tableDEN = "LDEN_RAILWAY"
        tableNIGHT = "LNIGHT_RAILWAY"
    }
    else{
        source = "ROADS"
        tableDEN = "LDEN_ROADS"
        tableNIGHT = "LNIGHT_ROADS"
    }

    String ldenInput = "RECEIVERS_DELAUNAY_DEN"
    String lnightInput = "RECEIVERS_DELAUNAY_NIGHT"

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)

    // -------------------
    // Initialisation des tables dans lesquelles on stockera les surfaces par tranche d'iso, par type d'infra et d'indice

    // Process each rail or road infrastructures
    sql.eachRow("SELECT DISTINCT UUEID FROM " + source + " ORDER BY UUEID ASC") { row ->
        String uueid = row[0] as String

        String ldenOutput = uueid + "_CONTOURING_LDEN"
        String lnightOutput = uueid + "_CONTOURING_LNIGHT"

        sql.execute(String.format("DROP TABLE IF EXISTS "+ ldenOutput +", "+ lnightOutput +", RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN"))

        logger.info(String.format("Create RECEIVERS_DELAUNAY_NIGHT for uueid= %s", uueid))
        sql.execute("create table RECEIVERS_DELAUNAY_NIGHT(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, 10*log10(sum(POWER(10, LAEQ / 10))) as LAEQ FROM "+tableNIGHT+" L USE INDEX ("+indexNight+") INNER JOIN "+source+" R ON L.IDSOURCE = R.PK INNER JOIN RECEIVERS RE ON L.IDRECEIVER = RE.PK WHERE R.UUEID='"+uueid+"' AND RE.RCV_TYPE = 2 GROUP BY RE.PK_1, RE.THE_GEOM;")
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_NIGHT ADD PRIMARY KEY (PK)")
        logger.info(String.format("Create RECEIVERS_DELAUNAY_DEN for uueid= %s", uueid))
        sql.execute("create table RECEIVERS_DELAUNAY_DEN(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, 10*log10(sum(POWER(10, LAEQ / 10))) as LAEQ FROM "+tableDEN+" L USE INDEX ("+indexDen+") INNER JOIN "+source+" R ON L.IDSOURCE = R.PK INNER JOIN RECEIVERS RE ON L.IDRECEIVER = RE.PK WHERE R.UUEID='"+uueid+"' AND RE.RCV_TYPE = 2 GROUP BY RE.PK_1, RE.THE_GEOM;")
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_DEN ADD PRIMARY KEY (PK)")


        logger.info("Generate iso surfaces")
        // For A maps
        // Produce isocontours for LNIGHT (LN)
        generateIsoSurfaces(lnightInput, isoLevelsLNIGHT, connection, uueid, 'A', 'LN', input)
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoLevelsLDEN, connection, uueid, 'A', 'LD', input)

        // For C maps
        // Produce isocontours for LNIGHT (LN)
        generateIsoSurfaces(lnightInput, isoCLevelsLNIGHT, connection, uueid, 'C', 'LN', input)
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoCLevelsLDEN, connection, uueid, 'C', 'LD', input)

        if (railRoad==1){
            generateIsoSurfaces(lnightInput, isoCFerConvLevelsLNIGHT, connection, uueid, 'C', 'LN', input)

            generateIsoSurfaces(ldenInput, isoCFerConvLevelsLDEN, connection, uueid, 'C', 'LD', input)
        }



        sql.execute("DROP TABLE IF EXISTS RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN")

        prog.endStep()
    }

     */
    List<Double> isoLevels = BezierContouring.NF31_133_ISO // default values
    // List of input tables : inputTable
    String source;
    String [] tables = new String[2]
    if (railRoad==1){
        source = "RAIL_SECTIONS"
        tables[0] = "LDEN_RAILWAY"
        tables[1] = "LNIGHT_RAILWAY"
    }
    else{
        source = "ROAD_TRAFFIC"
        tables[0] = "LDEN_ROADS"
        tables[1] = "LNIGHT_ROADS"
    }
    int srid = GeometryTableUtilities.getSRID(connection, TableLocation.parse(source))
    String resultString = "";
    try {
        for (int i = 0; i<2; i++){
            BezierContouring bezierContouring = new BezierContouring(isoLevels, srid)
            sql.execute("DROP TABLE IF EXISTS CONTOURING_NOISE_MAP")
            bezierContouring.setPointTable(tables[i])
            //bezierContouring.setTriangleTable("TRIANGLES")
            bezierContouring.setSmooth(false)
            bezierContouring.createTable(connection)
            sql.execute("CREATE TABLE "+ tables[i]+"_GEOM AS SELECT * FROM CONTOURING_NOISE_MAP")
            resultString += "Table "+tables[i]+"_GEOM created. "
        }
    } catch (Exception e) {
        logger.info("ERROR : "+ e.toString())
        return e.toString()
    }

    logger.info("This is the end of the step 5")
    // print to WPS Builder
    return resultString
}

/**
 * Generate isosurfaces for LDEN and LNIGHT tables
 * @param inputTable
 * @param isoClasses
 * @param connection
 * @param uueid
 * @param cbsType 
 * @param period
 * @param input
 * @return
 */
def generateIsoSurfaces(def inputTable, def isoClasses, def connection, String uueid, String cbsType, String period, def input) {

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    if(!JDBCUtilities.tableExists(connection, inputTable)) {
        logger.info "La table $inputTable n'est pas présente"
        return
        }
        int srid = GeometryTableUtilities.getSRID(connection, TableLocation.parse(inputTable))

        Integer railRoad = input["rail_or_road"]

        BezierContouring bezierContouring = new BezierContouring(isoClasses, srid)

        bezierContouring.setPointTable(inputTable)
        bezierContouring.setTriangleTable("TRIANGLES_DELAUNAY")
        bezierContouring.setSmooth(false)

        bezierContouring.createTable(connection)

        Sql sql = new Sql(connection)

        //String source = sql.firstRow("SELECT source FROM METADATA").source
        // for A maps
        String cbsARoadLden = "CBS_A_R_LD_"+source
        String cbsARoadLnight = "CBS_A_R_LN_"+source
        String cbsAFerLden = "CBS_A_F_LD_"+source
        String cbsAFerLnight = "CBS_A_F_LN_"+source

        // for C maps
        String cbsCRoadLden = "CBS_C_R_LD_"+source
        String cbsCRoadLnight = "CBS_C_R_LN_"+source
        String cbsCFerLGVLden = "CBS_C_F_LGV_LD_"+source
        String cbsCFerLGVLnight = "CBS_C_F_LGV_LN_"+source
        String cbsCFerCONVLden = "CBS_C_F_CONV_LD_"+source
        String cbsCFerCONVLnight = "CBS_C_F_CONV_LN_"+source

        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE CONTOURING_NOISE_MAP SET THE_GEOM=ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))")
        
        // Generate temporary table to store ISO areas
        sql.execute("DROP TABLE IF EXISTS ISO_AREA")
        sql.execute("CREATE TABLE ISO_AREA (the_geom geometry, pk varchar, UUEID varchar, CBSTYPE varchar, PERIOD varchar, noiselevel varchar, AREA float) AS SELECT ST_ACCUM(the_geom) the_geom, null, '"+uueid+"', '"+cbsType+"', '"+period+"', ISOLABEL, SUM(ST_AREA(the_geom)) AREA FROM CONTOURING_NOISE_MAP GROUP BY ISOLABEL")

        // For A maps
        // Update noise classes for LDEN
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '55-60' THEN 'Lden5559' WHEN NOISELEVEL = '60-65' THEN 'Lden6064' WHEN NOISELEVEL = '65-70' THEN 'Lden6569' WHEN NOISELEVEL = '70-75' THEN 'Lden7074' WHEN NOISELEVEL = '> 75' THEN 'LdenGreaterThan75' END) WHERE CBSTYPE = 'A' AND PERIOD='LD';")
        // Update noise classes for LNIGHT
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '50-55' THEN 'Lnight5054' WHEN NOISELEVEL = '55-60' THEN 'Lnight5559' WHEN NOISELEVEL = '60-65' THEN 'Lnight6064' WHEN NOISELEVEL = '65-70' THEN 'Lnight6569' WHEN NOISELEVEL = '> 70' THEN 'LnightGreaterThan70' END) WHERE CBSTYPE = 'A' AND PERIOD='LN';")

        // For C maps
        // Update noise classes for LDEN
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 68' THEN 'LdenGreaterThan68' WHEN NOISELEVEL = '> 73' THEN 'LdenGreaterThan73' END) WHERE CBSTYPE = 'C' AND PERIOD='LD';")
        // Update noise classes for LNIGHT
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 62' THEN 'LnightGreaterThan62' WHEN NOISELEVEL = '> 65' THEN 'LnightGreaterThan65' END) WHERE CBSTYPE = 'C' AND PERIOD='LN';")


        sql.execute("DELETE FROM ISO_AREA WHERE NOISELEVEL IS NULL");

        // Generate the PK
        sql.execute("UPDATE ISO_AREA SET pk = CONCAT(uueid, '_',noiselevel)")
        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE ISO_AREA SET THE_GEOM = ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))")

        // Insert iso areas into common table, according to rail or road input parameter
        if (railRoad==1){
            sql.execute("INSERT INTO "+cbsAFerLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LD'")
            sql.execute("INSERT INTO "+cbsAFerLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LN'")
            sql.execute("INSERT INTO "+cbsCFerLGVLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD' AND NOISELEVEL = 'LdenGreaterThan68'")
            sql.execute("INSERT INTO "+cbsCFerLGVLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN' AND NOISELEVEL = 'LnightGreaterThan62'")
            sql.execute("INSERT INTO "+cbsCFerCONVLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD' AND NOISELEVEL = 'LdenGreaterThan73'")
            sql.execute("INSERT INTO "+cbsCFerCONVLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN' AND NOISELEVEL = 'LnightGreaterThan65'")
        } else {
            sql.execute("INSERT INTO "+cbsARoadLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LD'")
            sql.execute("INSERT INTO "+cbsARoadLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LN'")
            sql.execute("INSERT INTO "+cbsCRoadLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD'")
            sql.execute("INSERT INTO "+cbsCRoadLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN'")
        }

        sql.execute("DROP TABLE IF EXISTS CONTOURING_NOISE_MAP")

        logger.info('End : Compute Isosurfaces')
}

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


