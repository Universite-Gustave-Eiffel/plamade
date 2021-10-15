package org.noise_planet.nmcluster;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.noise_planet.noisemodelling.jdbc.*;
import org.noise_planet.noisemodelling.pathfinder.IComputeRaysOut;
import org.noise_planet.noisemodelling.pathfinder.utils.JVMMemoryMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.ProfilerThread;
import org.noise_planet.noisemodelling.pathfinder.utils.ProgressMetric;
import org.noise_planet.noisemodelling.pathfinder.utils.ReceiverStatsMetric;
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NoiseModellingInstance {
    Connection connection;
    String workingDirectory;
    int configurationId = 0;

    public NoiseModellingInstance(Connection connection, String workingDirectory) {
        this.connection = connection;
        this.workingDirectory = workingDirectory;
    }

    public int getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(int configurationId) {
        this.configurationId = configurationId;
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

    public void roadNoiseLevel(ProgressVisitor progressLogger, List<PointNoiseMap.CellIndex> cellIndexList) throws SQLException, IOException {
        Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);


        PointNoiseMap pointNoiseMap = new PointNoiseMap("buildings_screens",
                "LW_ROADS_UEEID", "RECEIVERS_UEEID");


        LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

        Sql sql = new Sql(connection);
        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int reflectionOrder = (Integer)rs.get("confreflorder");
        int maxSrcDist = (Integer)rs.get("confmaxsrcdist");
        // overwrite with the system number of thread - 1
        Runtime runtime = Runtime.getRuntime();
        int nThread = Math.max(1, runtime.availableProcessors() - 1);

        boolean compute_vertical_diffraction = (Boolean)rs.get("confdiffvertical");
        boolean compute_horizontal_diffraction = (Boolean)rs.get("confdiffhorizontal");
        ldenConfig_propa.setComputeLDay(!(Boolean)rs.get("confskiplday"));
        ldenConfig_propa.setComputeLEvening(!(Boolean)rs.get("confskiplevening"));
        ldenConfig_propa.setComputeLNight(!(Boolean)rs.get("confskiplnight"));
        ldenConfig_propa.setComputeLDEN(!(Boolean)rs.get("confskiplden"));
        ldenConfig_propa.setMergeSources(true);
        ldenConfig_propa.setlDayTable("LDAY_ROADS");
        ldenConfig_propa.setlEveningTable("LEVENING_ROADS");
        ldenConfig_propa.setlNightTable("LNIGHT_ROADS");
        ldenConfig_propa.setlDenTable("LDEN_ROADS");
        ldenConfig_propa.setComputeLAEQOnly(true);

        LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig_propa);



        pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
        pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
        pointNoiseMap.setSoundReflectionOrder(reflectionOrder)

        // Set environmental parameters
        PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);


        GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");

        double confHumidity = (Double)row_zone.get("hygro_d");
        double confTemperature = (Double)row_zone.get("temp_d");
        String confFavorableOccurrences = (String)row_zone.get("pfav_06_18");

        environmentalData.setHumidity(confHumidity);
        environmentalData.setTemperature(confTemperature);

        StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ",");
        double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length];
        for (int i = 0; i < favOccurrences.length; i++) {
            favOccurrences[i] = Math.max(0, Math.min(1, Double.parseDouble(tk.nextToken().trim())));
        }
        environmentalData.setWindRose(favOccurrences);

        pointNoiseMap.setPropagationProcessPathData(environmentalData);

        pointNoiseMap.setThreadCount(nThread);
        // Building height field name
        pointNoiseMap.setHeightField("HEIGHT");
        // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
        pointNoiseMap.setSoilTableName("landcover");
        // Point cloud height above sea level POINT(X Y Z)
        pointNoiseMap.setDemTable("DEM");
        ProfilerThread profilerThread = new ProfilerThread(new File(workingDirectory, "profile.csv"));
        profilerThread.addMetric(ldenProcessing);
        profilerThread.addMetric(new ProgressMetric(progressLogger));
        profilerThread.addMetric(new JVMMemoryMetric());
        profilerThread.addMetric(new ReceiverStatsMetric());
        pointNoiseMap.setProfilerThread(profilerThread);

        final List<String> uueidList = new ArrayList<>();
        sql.rows("SELECT DISTINCT UUEID FROM SOURCE ORDER BY UUEID ASC").forEach(
                (groovyRowResult) -> uueidList.add((String)groovyRowResult.get("UUEID")));


        ProgressVisitor ueeidVisitor = progressLogger.subProcess(uueidList.size());
        for(String uueid : uueidList) {
            // Keep only receivers near selected UUEID
            sql.execute("DROP TABLE IF EXISTS RECEIVERS_UUEID");
            sql.execute("CREATE TABLE RECEIVERS_UUEID (THE_GEOM geometry, PK integer PRIMARY KEY, PK_1 integer, RCV_TYPE integer);");
            sql.execute("INSERT INTO RECEIVERS_UUEID (THE_GEOM ,PK, PK_1 , RCV_TYPE) SORTED SELECT THE_GEOM, PK, PK_1, 2 FROM RECEIVERS R WHERE EXISTS (SELECT 1 FROM ROADS S WHERE ST_EXPAND(R.THE_GEOM," + (double) maxSrcDist + ", " + (double) maxSrcDist + ") && S.THE_GEOM AND ST_DISTANCE(S.THE_GEOM, R.THE_GEOM) < " + (double) maxSrcDist + " AND UEEID = '"+uueid+"' LIMIT 1 ) ORDER BY PK");
            sql.execute("CREATE INDEX RECEIVERS_UUEID_PK1 ON RECEIVERS_UUEID(PK_1)");
            sql.execute("CREATE SPATIAL INDEX ON RECEIVERS_UUEID_PK1(THE_GEOM)");
            // Filter only sound source that match the UUEID
            sql.execute("DROP TABLE IF EXISTS LW_ROADS_UEEID");
            sql.execute("CREATE TABLE LW_ROADS_UEEID AS SORTED SELECT LW.* FROM LW_ROADS LW, ROADS R WHERE LW.PK = R.PK AND R.UUEID = '"+uueid+"' ORDER BY LW.PK");
            sql.execute("ALTER TABLE LW_ROADS_UEEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE SPATIAL INDEX ON LW_ROADS_UEEID(THE_GEOM)");

            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>();

            try {
                ldenProcessing.start();
                new Thread(profilerThread).start();
                // Iterate over computation areas
                AtomicInteger k = new AtomicInteger();
                Map<PointNoiseMap.CellIndex, Integer> cells = pointNoiseMap.searchPopulatedCells(connection);
                ProgressVisitor progressVisitor = ueeidVisitor.subProcess(cells.size());
                for (PointNoiseMap.CellIndex cellIndex : cells.keySet()) {// Run ray propagation
                    logger.info(String.format("Compute... %.3f %% (%d receivers in this cell)", 100.0 * (k.getAndIncrement() / cells.size()), cells.get(cellIndex)));
                    IComputeRaysOut ro = pointNoiseMap.evaluateCell(connection, cellIndex.getLatitudeIndex(), cellIndex.getLongitudeIndex(), progressVisitor, receivers);
                }
            } catch (IllegalArgumentException | IllegalStateException ex) {
                System.err.println(ex);
                throw ex;
            } finally {
                profilerThread.stop();
                ldenProcessing.stop();
            }
            // TODO ISOContour
        }
    }
}
