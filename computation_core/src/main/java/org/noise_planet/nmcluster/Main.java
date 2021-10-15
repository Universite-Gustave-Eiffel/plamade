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
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.sql.Sql;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


public class Main {

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Objects.requireNonNull(
                Main.class.getResource("log4j.properties")).getFile());

        Logger logger = LoggerFactory.getLogger("org.noise_planet");


        try {
            // Read node id parameter
            int nodeId = -1;
            String workingDir = "";
            for (int i = 0; args != null && i < args.length; i++) {
                String a = args[i];
                if(a == null) {
                    continue;
                }
                if (a.startsWith("-n")) {
                    nodeId = Integer.parseInt(a.substring(2));
                } else if (a.startsWith("-w")) {
                    workingDir = a.substring(2);
                    if(!(new File(workingDir).exists())) {
                        logger.error(workingDir + " folder does not exists");
                        workingDir = "";
                    }
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
            JsonNode node = mapper.readTree(new File(workingDir, "cluster_config.json"));
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
            DataSource ds = NoiseModellingInstance.createDataSource("", "", new File(workingDir).getAbsolutePath(), "h2gisdb", false);

            try (Connection connection = ds.getConnection()) {
                //RoadNoiselevel(connection, new RootProgressVisitor(1, true, 1.0), cellIndexList);
                // Fetch configuration ID
                Sql sql = new Sql(connection);
                int confId = (Integer)sql.firstRow("SELECT grid_conf from metadata").get("GRID_CONF");
                NoiseModellingInstance nm = new NoiseModellingInstance(connection, workingDir);
                nm.setConfigurationId(confId);
                nm.roadNoiseLevel(new RootProgressVisitor(1, true,
                        1), cellIndexList);
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