package org.noise_planet.nmcluster;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.EmptyProgressVisitor;
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
    Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
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

    public void uueidsLoop(ProgressVisitor progressLogger, List<String> uueidList) throws SQLException, IOException {
        Sql sql = new Sql(connection);

        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int maxSrcDist = (Integer)rs.get("confmaxsrcdist");

        for(String uueid : uueidList) {
            // Keep only receivers near selected UUEID
            logger.info("Fetch receivers near roads with uueid " + uueid);
            sql.execute("DROP TABLE IF EXISTS RECEIVERS_UUEID");
            sql.execute("CREATE TABLE RECEIVERS_UUEID (THE_GEOM geometry, PK integer not null, PK_1 integer, RCV_TYPE integer);");
            sql.execute("INSERT INTO RECEIVERS_UUEID SELECT R.* FROM RECEIVERS R, (SELECT st_accum(roads.the_geom) the_geom FROM ROADS WHERE UUEID = '"+uueid+"' GROUP BY UUEID) R2 WHERE R.THE_GEOM && ST_EXPAND(R2.the_geom," + (double) maxSrcDist + ", " + (double) maxSrcDist + ") AND ST_DISTANCE(R2.THE_GEOM, R.THE_GEOM) < " + (double) maxSrcDist);
            sql.execute("ALTER TABLE RECEIVERS_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE INDEX RECEIVERS_UUEID_PK1 ON RECEIVERS_UUEID(PK_1)");
            sql.execute("CREATE SPATIAL INDEX RECEIVERS_UUEID_SPI ON RECEIVERS_UUEID (THE_GEOM)");
            // Filter only sound source that match the UUEID
            logger.info("Fetch sound sources that match with uueid " + uueid);
            sql.execute("DROP TABLE IF EXISTS LW_ROADS_UUEID");
            sql.execute("CREATE TABLE LW_ROADS_UUEID AS SELECT LW.* FROM LW_ROADS LW, ROADS R WHERE LW.PK = R.PK AND R.UUEID = '" + uueid + "'");
            sql.execute("ALTER TABLE LW_ROADS_UUEID ALTER COLUMN PK INTEGER NOT NULL");
            sql.execute("ALTER TABLE LW_ROADS_UUEID ADD PRIMARY KEY(PK)");
            sql.execute("CREATE SPATIAL INDEX ON LW_ROADS_UUEID(THE_GEOM)");
            int nbSources = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM LW_ROADS_UUEID").get("CPT"));
            logger.info(String.format(Locale.ROOT, "There is %d sound sources with this UUEID", nbSources));
            if(nbSources > 0) {
                ProgressVisitor uueidVisitor = progressLogger.subProcess(uueidList.size());
                roadNoiseLevel(uueidVisitor, uueid);
            }

            break;
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

    public void roadNoiseLevel(ProgressVisitor progressLogger, String uueid) throws SQLException, IOException {


        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_SCREENS",
                "LW_ROADS_UUEID", "RECEIVERS_UUEID");


        LDENConfig ldenConfig_propa = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

        Sql sql = new Sql(connection);
        GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
        int reflectionOrder = asInteger(rs.get("confreflorder"));
        int maxSrcDist = asInteger(rs.get("confmaxsrcdist"));
        int maxReflectionDistance = asInteger(rs.get("confmaxrefldist"));
        double wallAlpha = asDouble(rs.get("wall_alpha"));
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



        pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction);
        pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction);
        pointNoiseMap.setSoundReflectionOrder(reflectionOrder);

        // Set environmental parameters
        PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);


        GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");

        double confHumidity = asDouble(row_zone.get("hygro_d"));
        double confTemperature = asDouble(row_zone.get("temp_d"));
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


        pointNoiseMap.setMaximumPropagationDistance(maxSrcDist);
        pointNoiseMap.setMaximumReflectionDistance(maxReflectionDistance);
        pointNoiseMap.setWallAbsorption(wallAlpha);


        ProfilerThread profilerThread = new ProfilerThread(new File(workingDirectory, "profile_"+uueid+".csv"));
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
