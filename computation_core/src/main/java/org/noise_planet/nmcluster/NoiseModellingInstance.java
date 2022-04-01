package org.noise_planet.nmcluster;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.cts.crs.CRSException;
import org.cts.op.CoordinateOperationException;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.noise_planet.noisemodelling.jdbc.BezierContouring;
import org.noise_planet.noisemodelling.jdbc.LDENComputeRaysOut;
import org.noise_planet.noisemodelling.jdbc.LDENConfig;
import org.noise_planet.noisemodelling.jdbc.LDENPointNoiseMapFactory;
import org.noise_planet.noisemodelling.jdbc.LDENPropagationProcessData;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.IComputeRaysOut;
import org.noise_planet.noisemodelling.pathfinder.utils.JVMMemoryMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.KMLDocument;
import org.noise_planet.noisemodelling.pathfinder.utils.ProfilerThread;
import org.noise_planet.noisemodelling.pathfinder.utils.ProgressMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.ReceiverStatsMetric;
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processing of noise maps
 * @author Pierre Aumond, Université Gustave Eiffel
 * @author Nicolas Fortin, Université Gustave Eiffel
 * @author Gwendall Petit, Lab-STICC CNRS UMR 6285
 */
public class NoiseModellingInstance {
    public enum SOURCE_TYPE { SOURCE_TYPE_RAIL, SOURCE_TYPE_ROAD}
    Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
    Connection connection;
    String workingDirectory;
    int configurationId = 0;
    String outputPrefix = "";
    boolean isExportDomain = false;
    String outputFolder;

    // LDEN classes for A maps : 55-59, 60-64, 65-69, 70-74 et >75 dB
    final List<Double> isoLevelsLDEN = Arrays.asList(55.0d,60.0d,65.0d,70.0d,75.0d,200.0d);
    // LDEN classes for A maps : >55 dB
    final List<Double> isoLevelsLDEN55 = Arrays.asList(55.0d,200.0d);
    // LDEN classes for A maps : >65 dB
    final List<Double> isoLevelsLDEN65 = Arrays.asList(65.0d,200.0d);
    // LNIGHT classes for A maps : 50-54, 55-59, 60-64, 65-69 et >70 dB
    final List<Double>  isoLevelsLNIGHT = Arrays.asList(50.0d,55.0d,60.0d,65.0d,70.0d,200.0d);

    // LDEN classes for C maps: >68 dB
    final List<Double>  isoCLevelsLDEN = Arrays.asList(68.0d,200.0d);
    // LNIGHT classes for C maps : >62 dB
    final List<Double>  isoCLevelsLNIGHT = Arrays.asList(62.0d,200.0d);

    // LDEN classes for C maps and : >73 dB
    final List<Double>  isoCFerConvLevelsLDEN= Arrays.asList(73.0d,200.0d);
    // LNIGHT classes for C maps : >65 dB
    final List<Double>  isoCFerConvLevelsLNIGHT= Arrays.asList(65.0d,200.0d);

    String cbsARoadLden;
    String cbsARoadLnight;
    String cbsAFerLden;
    String cbsAFerLnight;
    String cbsCRoadLden;
    String cbsCRoadLnight;
    String cbsCFerLGVLden;
    String cbsCFerLGVLnight;
    String cbsCFerCONVLden;
    String cbsCFerCONVLnight;


    public NoiseModellingInstance(Connection connection, String workingDirectory) {
        this.connection = connection;
        this.workingDirectory = workingDirectory;
        this.outputFolder = workingDirectory;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public int getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(int configurationId) {
        this.configurationId = configurationId;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }

    public static DataSource createDataSource(String user, String password, String dbDirectory, String dbName, boolean debug) throws SQLException {
        // Create H2 memory DataSource
        org.h2.Driver driver = org.h2.Driver.load();
        OsgiDataSourceFactory dataSourceFactory = new OsgiDataSourceFactory(driver);
        Properties properties = new Properties();
        String databasePath = "jdbc:h2:" + new File(dbDirectory, dbName).getAbsolutePath();
        properties.setProperty(DataSourceFactory.JDBC_URL, databasePath);
        properties.setProperty(DataSourceFactory.JDBC_USER, user);
        properties.setProperty(DataSourceFactory.JDBC_PASSWORD, password);
        if(debug) {
            properties.setProperty("TRACE_LEVEL_FILE", "3"); // enable debug
        }
        DataSource dataSource = dataSourceFactory.createDataSource(properties);
        // Init spatial ext
        try (Connection connection = dataSource.getConnection()) {
            H2GISFunctions.load(connection);
        }
        return dataSource;

    }

    public void exportDomain(LDENPropagationProcessData inputData, String path, int epsg) {
        try {
            logger.info("Export domain : Cell number " + inputData.cellId);
            FileOutputStream outData = new FileOutputStream(path);
            KMLDocument kmlDocument = new KMLDocument(outData);
            kmlDocument.setInputCRS("EPSG:" + epsg);
            kmlDocument.writeHeader();
            kmlDocument.writeTopographic(inputData.profileBuilder.getTriangles(), inputData.profileBuilder.getVertices());
            kmlDocument.writeBuildings(inputData.profileBuilder);
            kmlDocument.writeFooter();
        } catch (IOException | CRSException | XMLStreamException | CoordinateOperationException ex) {
            logger.error("Error while exporting domain", ex);
        }
    }

    /**
     *
     * @param progressLogger Progression instance
     * @param uueidList List of UUEID
     * @param sourceType source type
     * @throws SQLException
     * @throws IOException
     */
    public void uueidsLoop(ProgressVisitor progressLogger, List<String> uueidList, SOURCE_TYPE sourceType) throws SQLException, IOException {
        File outputDir = new File(outputFolder);
        if(!outputDir.exists()) {
            if(!outputDir.mkdir()) {
                logger.error("Cannot create " + outputDir.getAbsolutePath());
            }
        }
        if(uueidList.isEmpty()) {
            return;
        }
        Sql sql = new Sql(connection);

        List<String> outputTable = recreateCBS(sourceType);

        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int maxSrcDist = (Integer)rs.get("confmaxsrcdist");

        ProgressVisitor uueidVisitor = progressLogger.subProcess(uueidList.size());
        for(String uueid : uueidList) {
            // Keep only receivers near selected UUEID
            String conditionReceiver = "";
            // keep only receiver from contouring noise map
            conditionReceiver = " RCV_TYPE = 2 AND ";
            sql.execute("DROP TABLE IF EXISTS SOURCES_SPLITED;");
            if(sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) FROM RAIL_SECTIONS WHERE UUEID = ''"+uueid+"'')');");
            } else {
                sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) THE_GEOM FROM ROADS WHERE UUEID = ''"+uueid+"'')');");
            }
            logger.info("Fetch receivers near uueid " + uueid);
            sql.execute("CREATE SPATIAL INDEX ON SOURCES_SPLITED(THE_GEOM)");
            sql.execute("DROP TABLE IF EXISTS RECEIVERS_UUEID");
            sql.execute("CREATE TABLE RECEIVERS_UUEID(THE_GEOM geometry, PK integer not null, PK_1 integer, RCV_TYPE integer) AS SELECT R.* FROM RECEIVERS R WHERE "+conditionReceiver+" EXISTS (SELECT 1 FROM SOURCES_SPLITED R2 WHERE ST_EXPAND(R.THE_GEOM, "+maxSrcDist+", "+maxSrcDist+") && R2.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, R2.THE_GEOM) <= "+maxSrcDist+" LIMIT 1)");
            logger.info("Create primary key and index on filtered receivers");
            sql.execute("ALTER TABLE RECEIVERS_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE INDEX RECEIVERS_UUEID_PK1 ON RECEIVERS_UUEID(PK_1)");
            sql.execute("CREATE SPATIAL INDEX RECEIVERS_UUEID_SPI ON RECEIVERS_UUEID (THE_GEOM)");
            // Filter only sound source that match the UUEID
            logger.info("Fetch sound sources that match with uueid " + uueid);
            sql.execute("DROP TABLE IF EXISTS LW_UUEID");
            if(sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_RAILWAY LW WHERE UUEID = '" + uueid + "'");
            } else {
                sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_ROADS LW, ROADS R WHERE LW.PK = R.PK AND R.UUEID = '" + uueid + "'");
            }
            sql.execute("ALTER TABLE LW_UUEID ALTER COLUMN PK INTEGER NOT NULL");
            sql.execute("ALTER TABLE LW_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE SPATIAL INDEX ON LW_UUEID(THE_GEOM)");
            int nbSources = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM LW_UUEID").get("CPT"));
            logger.info(String.format(Locale.ROOT, "There is %d sound sources with this UUEID", nbSources));
            int nbReceivers = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM RECEIVERS_UUEID").get("CPT"));
            logger.info(String.format(Locale.ROOT, "There is %d receivers with this UUEID", nbReceivers));


            if(nbSources > 0) {
                doPropagation(uueidVisitor, uueid, sourceType);
                isoSurface(uueid, sourceType);
            }
        }

        logger.info("Write output tables");
        for(String tableName : outputTable) {
            // Export result tables as GeoJSON
            sql.execute("CALL GEOJSONWRITE('" + new File(outputFolder,  outputPrefix + tableName + ".geojson").getAbsolutePath()+"', '" + tableName + "');");
        }
    }

    public static Double asDouble(Object v) {
        if(v instanceof Number) {
            return ((Number)v).doubleValue();
        } else {
            return null;
        }
    }

    public static Integer asInteger(Object v) {
        if(v instanceof Number) {
            return ((Number)v).intValue();
        } else {
            return null;
        }
    }

    /**
     *
     * @param sourceType Road = 0 Rail=1
     * @return CBS Tables
     * @throws SQLException
     */
    public List<String> recreateCBS(SOURCE_TYPE sourceType) throws SQLException {

        List<String> outputTables = new ArrayList<>();

        // List of input tables : inputTable

        Sql sql = new Sql(connection);

        // -------------------
        // Initialisation des tables dans lesquelles on stockera les surfaces par tranche d'iso, par type d'infra et d'indice

        String nuts = (String)sql.firstRow("SELECT NUTS FROM METADATA").get("NUTS");
        cbsARoadLden = "CBS_A_R_LD_"+nuts;
        cbsARoadLnight = "CBS_A_R_LN_"+nuts;
        cbsAFerLden = "CBS_A_F_LD_"+nuts;
        cbsAFerLnight = "CBS_A_F_LN_"+nuts;

        cbsCRoadLden = "CBS_C_R_LD_"+nuts;
        cbsCRoadLnight = "CBS_C_R_LN_"+nuts;
        cbsCFerLGVLden = "CBS_C_F_LGV_LD_"+nuts;
        cbsCFerLGVLnight = "CBS_C_F_LGV_LN_"+nuts;
        cbsCFerCONVLden = "CBS_C_F_CONV_LD_"+nuts;
        cbsCFerCONVLnight = "CBS_C_F_CONV_LN_"+nuts;

        // output string, the information given back to the user
        StringBuilder resultString = new StringBuilder("Le processus est terminé - Les tables de sortie sont ");



        // Tables are created according to the input parameter "rail" or "road"
        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            outputTables.add(cbsAFerLden);
            outputTables.add(cbsAFerLnight);
            outputTables.add(cbsCFerLGVLden);
            outputTables.add(cbsCFerLGVLnight);
            outputTables.add(cbsCFerCONVLden);
            outputTables.add(cbsCFerCONVLnight);
            // For A maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLden);
            sql.execute("CREATE TABLE "+ cbsAFerLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsAFerLnight);
            sql.execute("CREATE TABLE "+ cbsAFerLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");

            // For C maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLden);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerLGVLnight);
            sql.execute("CREATE TABLE "+ cbsCFerLGVLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");

            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerCONVLden);
            sql.execute("CREATE TABLE "+ cbsCFerCONVLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCFerCONVLnight);
            sql.execute("CREATE TABLE "+ cbsCFerCONVLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
        } else{
            outputTables.add(cbsARoadLden);
            outputTables.add(cbsARoadLnight);
            outputTables.add(cbsCRoadLden);
            outputTables.add(cbsCRoadLnight);
            // For A maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLden);
            sql.execute("CREATE TABLE "+ cbsARoadLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsARoadLnight);
            sql.execute("CREATE TABLE "+ cbsARoadLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            // For C maps
            sql.execute("DROP TABLE IF EXISTS "+ cbsCRoadLden);
            sql.execute("CREATE TABLE "+ cbsCRoadLden +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
            sql.execute("DROP TABLE IF EXISTS "+ cbsCRoadLnight);
            sql.execute("CREATE TABLE "+ cbsCRoadLnight +" (the_geom geometry, pk varchar, UUEID varchar, PERIOD varchar, noiselevel varchar, AREA float)");
        }
        for(String outputTable : outputTables) {
            resultString.append(", ").append(outputTable);
        }

        logger.info(resultString.toString());
        return outputTables;
    }

    void generateIsoSurfaces(String inputTable, List<Double> isoClasses, Connection connection, String uueid,
                             String cbsType, String period, SOURCE_TYPE sourceType) throws SQLException {

        if(!JDBCUtilities.tableExists(connection, inputTable)) {
            logger.info("La table "+inputTable+" n'est pas présente");
            return;
        }
        DBTypes dbTypes = DBUtils.getDBType(connection);
        int srid = GeometryTableUtilities.getSRID(connection, TableLocation.parse(inputTable, dbTypes));

        BezierContouring bezierContouring = new BezierContouring(isoClasses, srid);

        bezierContouring.setPointTable(inputTable);
        bezierContouring.setTriangleTable("TRIANGLES_DELAUNAY");
        bezierContouring.setSmooth(false);

        bezierContouring.createTable(connection);

        Sql sql = new Sql(connection);

        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE CONTOURING_NOISE_MAP SET THE_GEOM=ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))");

        // Generate temporary table to store ISO areas
        sql.execute("DROP TABLE IF EXISTS ISO_AREA");
        sql.execute("CREATE TABLE ISO_AREA (the_geom geometry, pk varchar, UUEID varchar, CBSTYPE varchar, PERIOD varchar, noiselevel varchar, AREA float) AS SELECT ST_ACCUM(the_geom) the_geom, null, '"+uueid+"', '"+cbsType+"', '"+period+"', ISOLABEL, SUM(ST_AREA(the_geom)) AREA FROM CONTOURING_NOISE_MAP GROUP BY ISOLABEL");

        // For A maps
        // Update noise classes for LDEN
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '55-60' THEN 'Lden5559' WHEN NOISELEVEL = '60-65' THEN 'Lden6064' WHEN NOISELEVEL = '65-70' THEN 'Lden6569' WHEN NOISELEVEL = '70-75' THEN 'Lden7074' WHEN NOISELEVEL = '> 55' THEN 'LdenGreaterThan55' WHEN NOISELEVEL = '> 65' THEN 'LdenGreaterThan65' WHEN NOISELEVEL = '> 75' THEN 'LdenGreaterThan75' END) WHERE CBSTYPE = 'A' AND PERIOD='LD';");
        // Update noise classes for LNIGHT
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '50-55' THEN 'Lnight5054' WHEN NOISELEVEL = '55-60' THEN 'Lnight5559' WHEN NOISELEVEL = '60-65' THEN 'Lnight6064' WHEN NOISELEVEL = '65-70' THEN 'Lnight6569' WHEN NOISELEVEL = '> 70' THEN 'LnightGreaterThan70' END) WHERE CBSTYPE = 'A' AND PERIOD='LN';");

        // For C maps
        // Update noise classes for LDEN
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 68' THEN 'LdenGreaterThan68' WHEN NOISELEVEL = '> 73' THEN 'LdenGreaterThan73' END) WHERE CBSTYPE = 'C' AND PERIOD='LD';");
        // Update noise classes for LNIGHT
        sql.execute("UPDATE ISO_AREA SET NOISELEVEL = (CASE WHEN NOISELEVEL = '> 62' THEN 'LnightGreaterThan62' WHEN NOISELEVEL = '> 65' THEN 'LnightGreaterThan65' END) WHERE CBSTYPE = 'C' AND PERIOD='LN';");


        sql.execute("DELETE FROM ISO_AREA WHERE NOISELEVEL IS NULL");

        // Generate the PK
        sql.execute("UPDATE ISO_AREA SET pk = CONCAT(uueid, '_',noiselevel)");
        // Forces the SRID, as it is lost in the previous steps
        sql.execute("UPDATE ISO_AREA SET THE_GEOM = ST_SetSRID(THE_GEOM, (SELECT SRID FROM METADATA))");

        // Insert iso areas into common table, according to rail or road input parameter
        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            sql.execute("INSERT INTO "+cbsAFerLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LD'");
            sql.execute("INSERT INTO "+cbsAFerLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LN'");
            sql.execute("INSERT INTO "+cbsCFerLGVLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD' AND NOISELEVEL = 'LdenGreaterThan68'");
            sql.execute("INSERT INTO "+cbsCFerLGVLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN' AND NOISELEVEL = 'LnightGreaterThan62'");
            sql.execute("INSERT INTO "+cbsCFerCONVLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD' AND NOISELEVEL = 'LdenGreaterThan73'");
            sql.execute("INSERT INTO "+cbsCFerCONVLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN' AND NOISELEVEL = 'LnightGreaterThan65'");
        } else {
            sql.execute("INSERT INTO "+cbsARoadLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LD'");
            sql.execute("INSERT INTO "+cbsARoadLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'A' AND PERIOD='LN'");
            sql.execute("INSERT INTO "+cbsCRoadLden+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LD'");
            sql.execute("INSERT INTO "+cbsCRoadLnight+" SELECT the_geom, pk, uueid, period, noiselevel, area FROM ISO_AREA WHERE CBSTYPE = 'C' AND PERIOD='LN'");
        }

        sql.execute("DROP TABLE IF EXISTS CONTOURING_NOISE_MAP");
        logger.info("End : Compute Isosurfaces");
    }

    public void isoSurface(String uueid, SOURCE_TYPE sourceType) throws SQLException {
        Sql sql = new Sql(connection);

        String tableDEN, tableNIGHT;
        if (sourceType== SOURCE_TYPE.SOURCE_TYPE_RAIL){
            tableDEN = "LDEN_RAILWAY";
            tableNIGHT = "LNIGHT_RAILWAY";
        } else{
            tableDEN = "LDEN_ROADS";
            tableNIGHT = "LNIGHT_ROADS";
        }

        String ldenOutput = uueid + "_CONTOURING_LDEN";
        String lnightOutput = uueid + "_CONTOURING_LNIGHT";

        sql.execute("DROP TABLE IF EXISTS "+ ldenOutput +", "+ lnightOutput +", RECEIVERS_DELAUNAY_NIGHT, RECEIVERS_DELAUNAY_DEN");

        logger.info(String.format("Create RECEIVERS_DELAUNAY_NIGHT for uueid= %s", uueid));
        sql.execute("create table RECEIVERS_DELAUNAY_NIGHT(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, LAEQ FROM "+tableNIGHT+" L INNER JOIN RECEIVERS_UUEID RE ON L.IDRECEIVER = RE.PK WHERE RE.RCV_TYPE = 2;");
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_NIGHT ADD PRIMARY KEY (PK)");
        logger.info(String.format("Create RECEIVERS_DELAUNAY_DEN for uueid= %s", uueid));
        sql.execute("create table RECEIVERS_DELAUNAY_DEN(PK INT NOT NULL, THE_GEOM GEOMETRY, LAEQ DECIMAL(6,2)) as SELECT RE.PK_1, RE.THE_GEOM, LAEQ FROM "+tableDEN+" L INNER JOIN RECEIVERS_UUEID RE ON L.IDRECEIVER = RE.PK WHERE RE.RCV_TYPE = 2;");
        sql.execute("ALTER TABLE RECEIVERS_DELAUNAY_DEN ADD PRIMARY KEY (PK)");



        logger.info("Generate iso surfaces");

        String ldenInput = "RECEIVERS_DELAUNAY_DEN";
        String lnightInput = "RECEIVERS_DELAUNAY_NIGHT";

        // For A maps
        // Produce isocontours for LNIGHT (LN)
        generateIsoSurfaces(lnightInput, isoLevelsLNIGHT, connection, uueid, "A", "LN", sourceType);
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoLevelsLDEN, connection, uueid, "A", "LD", sourceType);
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoLevelsLDEN55, connection, uueid, "A", "LD", sourceType);
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoLevelsLDEN65, connection, uueid, "A", "LD", sourceType);

        // For C maps
        // Produce isocontours for LNIGHT (LN)
        generateIsoSurfaces(lnightInput, isoCLevelsLNIGHT, connection, uueid, "C", "LN", sourceType);
        // Produce isocontours for LDEN (LD)
        generateIsoSurfaces(ldenInput, isoCLevelsLDEN, connection, uueid, "C", "LD", sourceType);

        if (sourceType == SOURCE_TYPE.SOURCE_TYPE_RAIL){
            generateIsoSurfaces(lnightInput, isoCFerConvLevelsLNIGHT, connection, uueid, "C", "LN", sourceType);
            generateIsoSurfaces(ldenInput, isoCFerConvLevelsLDEN, connection, uueid, "C", "LD", sourceType);
        }

        sql.execute("DROP TABLE IF EXISTS "+ldenInput + ", " + lnightInput);
    }

    public void doPropagation(ProgressVisitor progressLogger, String uueid, SOURCE_TYPE sourceType) throws SQLException, IOException {

        String sourceTable = "LW_UUEID";
        String receiversTable = "RECEIVERS_UUEID";

        DBTypes dbTypes = DBUtils.getDBType(connection);
        int sridBuildings = GeometryTableUtilities.getSRID(connection, TableLocation.parse("BUILDINGS_SCREENS", dbTypes));
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_SCREENS",
                sourceTable, receiversTable);


        LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

        Sql sql = new Sql(connection);
        sql.execute("UPDATE metadata SET road_start = NOW();");
        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int reflectionOrder = asInteger(rs.get("confreflorder"));
        int maxSrcDist = asInteger(rs.get("confmaxsrcdist"));
        int maxReflectionDistance = asInteger(rs.get("confmaxrefldist"));
        double wallAlpha = asDouble(rs.get("wall_alpha"));
        // overwrite with the system number of thread - 1
        Runtime runtime = Runtime.getRuntime();
        int nThread = Math.max(1, runtime.availableProcessors() - 1);

        boolean compute_vertical_edge_diffraction = (Boolean)rs.get("confdiffvertical");
        boolean compute_horizontal_edge_diffraction = (Boolean)rs.get("confdiffhorizontal");
        ldenConfig_propa.setComputeLDay(!(Boolean)rs.get("confskiplday"));
        ldenConfig_propa.setComputeLEvening(!(Boolean)rs.get("confskiplevening"));
        ldenConfig_propa.setComputeLNight(!(Boolean)rs.get("confskiplnight"));
        ldenConfig_propa.setComputeLDEN(!(Boolean)rs.get("confskiplden"));
        ldenConfig_propa.setMergeSources(true);
        if(sourceType == SOURCE_TYPE.SOURCE_TYPE_ROAD) {
            ldenConfig_propa.setlDayTable("LDAY_ROADS");
            ldenConfig_propa.setlEveningTable("LEVENING_ROADS");
            ldenConfig_propa.setlNightTable("LNIGHT_ROADS");
            ldenConfig_propa.setlDenTable("LDEN_ROADS");
        } else {
            ldenConfig_propa.setlDayTable("LDAY_RAILWAY");
            ldenConfig_propa.setlEveningTable("LEVENING_RAILWAY");
            ldenConfig_propa.setlNightTable("LNIGHT_RAILWAY");
            ldenConfig_propa.setlDenTable("LDEN_RAILWAY");
        }
        ldenConfig_propa.setComputeLAEQOnly(true);

        LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa);
        ldenProcessing.insertTrainDirectivity();

        // setComputeVerticalDiffraction its vertical diffraction (over horizontal edges)
        pointNoiseMap.setComputeVerticalDiffraction(compute_horizontal_edge_diffraction);
        pointNoiseMap.setComputeHorizontalDiffraction(compute_vertical_edge_diffraction);
        pointNoiseMap.setSoundReflectionOrder(reflectionOrder);

        // Set environmental parameters
        GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");
        String[] fieldTemperature = new String[] {"TEMP_D" ,"TEMP_E" ,"TEMP_N"};
        String[] fieldHumidity = new String[] {"HYGRO_D", "HYGRO_E", "HYGRO_N"};
        String[] fieldPFav = new String[] {"PFAV_06_18", "PFAV_18_22", "PFAV_22_06"};

        for(int idTime = 0; idTime < LDENConfig.TIME_PERIOD.values().length; idTime++) {
            PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);
            double confHumidity = asDouble(row_zone.get(fieldHumidity[idTime]));
            double confTemperature = asDouble(row_zone.get(fieldTemperature[idTime]));
            String confFavorableOccurrences = (String) row_zone.get(fieldPFav[idTime]);
            environmentalData.setHumidity(confHumidity);
            environmentalData.setTemperature(confTemperature);
            StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ",");
            double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length];
            for (int i = 0; i < favOccurrences.length; i++) {
                favOccurrences[i] = Math.max(0, Math.min(1, Double.parseDouble(tk.nextToken().trim())));
            }
            environmentalData.setWindRose(favOccurrences);
            pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.values()[idTime], environmentalData);
        }
        pointNoiseMap.setThreadCount(nThread);
        logger.info(String.format("PARAM : Number of thread used %d ", nThread));
        // Building height field name
        pointNoiseMap.setHeightField("HEIGHT");
        // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
        pointNoiseMap.setSoilTableName("landcover");
        // Point cloud height above sea level POINT(X Y Z)
        pointNoiseMap.setDemTable("DEM");


        pointNoiseMap.setMaximumPropagationDistance(maxSrcDist);
        pointNoiseMap.setMaximumReflectionDistance(maxReflectionDistance);
        pointNoiseMap.setWallAbsorption(wallAlpha);


        ProfilerThread profilerThread = new ProfilerThread(new File(outputFolder, "profile_"+uueid+".csv"));
        profilerThread.addMetric(ldenProcessing);
        profilerThread.addMetric(new ProgressMetric(progressLogger));
        profilerThread.addMetric(new JVMMemoryMetric());
        profilerThread.addMetric(new ReceiverStatsMetric());
        pointNoiseMap.setProfilerThread(profilerThread);

        // Set of already processed receivers
        Set<Long> receivers = new HashSet<>();

        // Do not propagate for low emission or far away sources
        // Maximum error in dB
        pointNoiseMap.setMaximumError(0.2d);

        // --------------------------------------------
        pointNoiseMap.setComputeRaysOutFactory(ldenProcessing);
        pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing);

        // Init Map
        pointNoiseMap.initialize(connection, new EmptyProgressVisitor());
        try {
            ldenProcessing.start();
            new Thread(profilerThread).start();
            // Iterate over computation areas
            AtomicInteger k = new AtomicInteger();
            Map<PointNoiseMap.CellIndex, Integer> cells = pointNoiseMap.searchPopulatedCells(connection);
            ProgressVisitor progressVisitor = progressLogger.subProcess(cells.size());
            for (PointNoiseMap.CellIndex cellIndex : new TreeSet<>(cells.keySet())) {// Run ray propagation
                logger.info(String.format("Compute... %d cells remaining (%d receivers in this cell)", cells.size() - k.getAndIncrement(), cells.get(cellIndex)));
                IComputeRaysOut ro = pointNoiseMap.evaluateCell(connection, cellIndex.getLatitudeIndex(), cellIndex.getLongitudeIndex(), progressVisitor, receivers);
                if(isExportDomain && ro instanceof LDENComputeRaysOut && ((LDENComputeRaysOut)ro).inputData instanceof LDENPropagationProcessData) {
                    String path = new File(outputFolder,
                            String.format("domain_%s_part_%d_%d.kml",uueid, cellIndex.getLatitudeIndex(),
                                    cellIndex.getLongitudeIndex())).getAbsolutePath();
                    exportDomain((LDENPropagationProcessData)(((LDENComputeRaysOut)ro).inputData), path,
                            sridBuildings);
                }
                if(progressVisitor.isCanceled()) {
                    throw new IllegalStateException("Calculation has been canceled, an error may have been raised");
                }
            }
        } finally {
            profilerThread.stop();
            ldenProcessing.stop();
        }
        sql.execute("UPDATE metadata SET road_end = NOW();");
    }

}



