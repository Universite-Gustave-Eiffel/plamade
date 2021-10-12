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
package org.noise_planet.nmcluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.log4j.PropertyConfigurator;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


public class Main {

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

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Objects.requireNonNull(
                Main.class.getResource("log4j.properties")).getFile());

        Logger logger = LoggerFactory.getLogger("org.noise_planet");


        try {
            // Read node id parameter
            int nodeId = -1;
            for (int i = 0; args != null && i < args.length; i++) {
                String a = args[i];
                if (a != null && a.startsWith("-n")) {
                    nodeId = Integer.parseInt(a.substring(2));
                }
            }
            if (nodeId == -1) {
                logger.info("Command line arguments :");
                for (String arg : args) {
                    logger.info("Got argument [" + arg + "]");
                }
                throw new IllegalArgumentException("Missing node identifier. -n[nodeId]");
            }
            // Load Json cluster configuration file
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(new File("cluster_config.json"));
            JsonNode v;
            List<PointNoiseMap.CellIndex> cellIndexList = new ArrayList<>();
            if (node instanceof ArrayNode) {
                ArrayNode aNode = (ArrayNode) node;
                for (JsonNode cellNode : aNode) {
                    v = cellNode.get("nodeIndex");
                    if (v != null && v.canConvertToInt() && v.intValue() == nodeId) {
                        cellIndexList.add(new PointNoiseMap.CellIndex(cellNode.get("longitudeIndex").intValue(), cellNode.get("latitudeIndex").intValue()));
                    }
                }
            }
            // Open database
            DataSource ds = createDataSource("", "", new File("./").getAbsolutePath(), "h2gisdb", false);

            try (Connection connection = ds.getConnection()) {
                RoadNoiselevel(connection, new RootProgressVisitor(1, true, 1.0), cellIndexList);


            } catch (SQLException ex) {
                while (ex != null) {
                    logger.error(ex.getLocalizedMessage(), ex);
                    ex = ex.getNextException();
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
    }


    public static Object RoadNoiselevel(Connection nmConnection, ProgressVisitor progressVisitor, List<PointNoiseMap.CellIndex> cellIndexList) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s4_Road_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", 4);
        //inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("outputToSql", false);
        inputs.put("cellsToProcess", cellIndexList);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }
}