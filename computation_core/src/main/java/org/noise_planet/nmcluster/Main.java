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
import groovy.sql.Sql;
import org.apache.log4j.PropertyConfigurator;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;


public class Main {
    public static final int SECONDS_BETWEEN_PROGRESSION_PRINT = 5;

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Main.class.getResource("log4j.properties"));

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
            List<String> uueidList = new ArrayList<>();
            if (node instanceof ArrayNode) {
                ArrayNode aNode = (ArrayNode) node;
                for (JsonNode cellNode : aNode) {
                    JsonNode nodeIdProp = cellNode.get("nodeId");
                    if(nodeIdProp != null && nodeIdProp.canConvertToInt() && nodeIdProp.intValue() == nodeId) {
                        if(cellNode.get("uueids") instanceof ArrayNode) {
                            for (JsonNode uueidNode : cellNode.get("uueids")) {
                                uueidList.add(uueidNode.asText());
                            }
                        }
                        break;
                    }
                }
            }
            logger.info(String.format(Locale.ROOT, "For job %d, will compute the following UUEID (%s)",
                    nodeId, String.join(",", uueidList)));
            // Open database
            DataSource ds = NoiseModellingInstance.createDataSource("", "", new File(workingDir).getAbsolutePath(), "h2gisdb", false);

            try (Connection connection = new ConnectionWrapper(ds.getConnection())) {
                //RoadNoiselevel(connection, new RootProgressVisitor(1, true, 1.0), cellIndexList);
                // Fetch configuration ID
                Sql sql = new Sql(connection);
                int confId = (Integer)sql.firstRow("SELECT grid_conf from metadata").get("GRID_CONF");
                NoiseModellingInstance nm = new NoiseModellingInstance(connection, workingDir);
                nm.setConfigurationId(confId);
                nm.setOutputPrefix(String.format(Locale.ROOT, "out_%d_", nodeId));
                nm.uueidsLoop(new RootProgressVisitor(1, true,
                        SECONDS_BETWEEN_PROGRESSION_PRINT), uueidList, 2);
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
}