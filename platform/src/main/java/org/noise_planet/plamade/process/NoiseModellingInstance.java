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

package org.noise_planet.plamade.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.plamade.config.DataBaseConfig;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This process hold one instance of NoiseModelling
 * @author Nicolas Fortin, Université Gustave Eiffel
 */
public class NoiseModellingInstance implements RunnableFuture<String> {
    Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
    Configuration configuration;
    DataSource nmDataSource;
    DataSource plamadeDataSource;
    boolean isRunning = false;
    boolean isCanceled = false;

    public NoiseModellingInstance(Configuration configuration, DataSource plamadeDataSource) {
        this.configuration = configuration;
        this.plamadeDataSource = plamadeDataSource;
    }

    public static DataSource createDataSource(String user, String password, String dbDirectory,String dbName, boolean debug) throws SQLException {
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

    public void importData(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {

        GroovyShell shell = new GroovyShell();
        Script extractDepartment= shell.parse(new File("../script_groovy", "s1_Extract_Department.groovy"));

        Map<String, Object> inputs = new HashMap<>();
        inputs.put("databaseUser", configuration.getDataBaseConfig().user);
        inputs.put("databasePassword", configuration.getDataBaseConfig().password);
        inputs.put("fetchDistance", 1000);
        inputs.put("inseeDepartment", configuration.getInseeDepartment());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("inputServer", "cloud");

        Object result = extractDepartment.invokeMethod("exec", new Object[] {nmConnection, inputs});

        if(result instanceof String) {
            try (OutputStreamWriter f = new OutputStreamWriter(new FileOutputStream(new File(configuration.getWorkingDirectory(), "import.html")), StandardCharsets.UTF_8)) {
                f.write(result.toString());
            }
        }
    }

    public void makeGrid(Connection nmConnection) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script receiversGrid= shell.parse(new File("../script_groovy", "s2_Receivers_Grid.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());

        Object result = receiversGrid.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public void makeEmission(Connection nmConnection) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script receiversGrid= shell.parse(new File("../script_groovy", "s3_Emission_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());

        Object result = receiversGrid.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }


    public Object RoadNoiselevel(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s4_Road_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("outputToSql", false);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public static JsonNode convertToJson(List<ArrayList<PointNoiseMap.CellIndex>> cells) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootNode = mapper.createArrayNode();
        int nodeIndex = 0;
        for(ArrayList<PointNoiseMap.CellIndex> node : cells) {
            for(PointNoiseMap.CellIndex cellIndex : node) {
                ObjectNode cellNode = mapper.createObjectNode();
                cellNode.put("nodeIndex", nodeIndex);
                cellNode.put("latitudeIndex", cellIndex.getLatitudeIndex());
                cellNode.put("longitudeIndex", cellIndex.getLongitudeIndex());
                rootNode.add(cellNode);
            }
            nodeIndex++;
        }
        return rootNode;
    }

    public static void generateClusterConfig(Connection nmConnection, ProgressVisitor progressVisitor, int numberOfNodes, String workingDirectory) throws SQLException, IOException {
        // Sum UUEID roads length
        // because the length of the roads should be proportional with the work load
        Map<String, Double> uueidToRoadLength = new HashMap<>();
        try(ResultSet rs = nmConnection.createStatement().executeQuery("SELECT UUEID , SUM(st_length(the_geom)) weight  FROM ROADS r GROUP BY uueid;")) {
            while(rs.next()) {
                uueidToRoadLength.put(rs.getString("UUEID"), rs.getDouble("WEIGHT"));
            }
        }
        double quota = uueidToRoadLength.values().stream().mapToDouble(Double::doubleValue).sum() / numberOfNodes;
        AtomicInteger nodeId = new AtomicInteger();
        double nodeTotalLength = 0;
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootDoc = mapper.createArrayNode();
        ObjectNode nodeDoc = mapper.createObjectNode();
        rootDoc.add(nodeDoc);
        nodeDoc.put("nodeId", nodeId.getAndIncrement());
        ArrayNode nodeUUEIDs = mapper.createArrayNode();
        nodeDoc.set("uueids", nodeUUEIDs);
        // create lists so that each node have at least the quota length of roads (except the last one)
        for (Map.Entry<String, Double> entry : uueidToRoadLength.entrySet()) {
            String uueid = entry.getKey();
            Double roadLength = entry.getValue();
            if (nodeTotalLength > quota) {
                nodeDoc.put("node_sum_length", nodeTotalLength);
                nodeDoc = mapper.createObjectNode();
                rootDoc.add(nodeDoc);
                nodeDoc.put("nodeId", nodeId.getAndIncrement());
                nodeUUEIDs = mapper.createArrayNode();
                nodeDoc.set("uueids", nodeUUEIDs);
                nodeTotalLength = 0;
            }
            nodeUUEIDs.add(uueid);
            nodeTotalLength += roadLength;
        }
        nodeDoc.put("node_sum_length", nodeTotalLength);
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(workingDirectory, "cluster_config.json"), rootDoc);
//        GroovyShell shell = new GroovyShell();
//        Script process= shell.parse(new File("../script_groovy", "s41_ClusterConfiguration.groovy"));
//        Map<String, Object> inputs = new HashMap<>();
//        inputs.put("confId", configuration.getConfigurationId());
//        inputs.put("numberOfNodes", 8);
//        inputs.put("progressVisitor", progressVisitor);
//        Object result = process.invokeMethod("exec", new Object[] {nmConnection, inputs});
//        if(result instanceof List) {
//            // Convert to json
//            List<ArrayList<PointNoiseMap.CellIndex>> cells = (List<ArrayList<PointNoiseMap.CellIndex>>)result;
//            JsonNode rootNode = convertToJson(cells);
//            ObjectMapper mapper = new ObjectMapper();
//            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(configuration.workingDirectory, "cluster_config.json"), rootNode);
//        }
    }
    public Object LoadNoiselevel(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s42_Load_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public Object Isosurface(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s5_Isosurface.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }


    public Object Export(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s7_Export.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("databaseUser", configuration.getDataBaseConfig().user);
        inputs.put("databasePassword", configuration.getDataBaseConfig().password);
        inputs.put("batchSize", 1000);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    @Override
    public void run() {
        Thread.currentThread().setName("JOB_" + configuration.getTaskPrimaryKey());
        isRunning = true;
        try {
            // create folder
            File workingDir = new File(configuration.workingDirectory);
            if (workingDir.exists() && workingDir.isDirectory()) {
                if (workingDir.getAbsolutePath().startsWith(new File("").getAbsolutePath())) {
                    if (!workingDir.delete()) {
                        logger.error("Cannot delete the working directory\n" + configuration.workingDirectory);
                        return;
                    }
                } else {
                    logger.error(String.format(Locale.ROOT, "Can delete only sub-folder \n%s\n%s", new File("").getAbsolutePath(), workingDir.getAbsolutePath()));
                }
            }
            if (!(workingDir.mkdirs())) {
                logger.error("Cannot create the working directory\n" + configuration.workingDirectory);
                return;
            }
            nmDataSource = createDataSource("", "", configuration.workingDirectory, "h2gisdb", false);

            // Download data from external database
            ProgressVisitor progressVisitor = configuration.progressVisitor;
            ProgressVisitor subProg = progressVisitor.subProcess(6);
            try (Connection nmConnection = nmDataSource.getConnection()) {
                importData(nmConnection, subProg);
                makeGrid(nmConnection);
                subProg.endStep();
                makeEmission(nmConnection);
                subProg.endStep();
                generateClusterConfig(nmConnection, subProg, 4, configuration.workingDirectory);
//                RoadNoiselevel(nmConnection, subProg);
//                //LoadNoiselevel(nmConnection, subProg);
//                Isosurface(nmConnection, subProg);
//                Export(nmConnection, subProg);
//                subProg.endStep();
            }
        } catch (SQLException ex) {
            while(ex != null) {
                logger.error(ex.getLocalizedMessage(), ex);
                ex = ex.getNextException();
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        } finally {
            // Update Job informations
            try (Connection connection = plamadeDataSource.getConnection()) {
                PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET END_DATE = ?, PROGRESSION = 100 WHERE PK_JOB = ?");
                st.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                st.setInt(2, configuration.getTaskPrimaryKey());
                st.execute();
            } catch (SQLException | SecurityException ex) {
                logger.error(ex.getLocalizedMessage(), ex);
            }
            isRunning = false;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        isCanceled = true;
        return false;
    }

    @Override
    public boolean isCancelled() {
        return isCanceled;
    }

    @Override
    public boolean isDone() {
        return !isRunning;
    }

    @Override
    public String get() throws InterruptedException, ExecutionException {
        return "";
    }

    @Override
    public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return "";
    }

    public static class Configuration {
        private String workingDirectory;
        private int configurationId;
        private String inseeDepartment;
        private int taskPrimaryKey;
        private DataBaseConfig dataBaseConfig;
        private ProgressVisitor progressVisitor = new EmptyProgressVisitor();

        public Configuration(String workingDirectory, int configurationId, String inseeDepartment, int taskPrimaryKey
                , DataBaseConfig dataBaseConfig, ProgressVisitor progressVisitor) {
            this.workingDirectory = workingDirectory;
            this.configurationId = configurationId;
            this.inseeDepartment = inseeDepartment;
            this.taskPrimaryKey = taskPrimaryKey;
            this.dataBaseConfig = dataBaseConfig;
            this.progressVisitor = progressVisitor;
        }

        public String getWorkingDirectory() {
            return workingDirectory;
        }

        public int getConfigurationId() {
            return configurationId;
        }

        public String getInseeDepartment() {
            return inseeDepartment;
        }

        public int getTaskPrimaryKey() {
            return taskPrimaryKey;
        }

        public DataBaseConfig getDataBaseConfig() {
            return dataBaseConfig;
        }
    }
}
