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
import org.h2gis.functions.io.geojson.GeoJsonWrite;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Main {
    public static final int SECONDS_BETWEEN_PROGRESSION_PRINT = 5;

    public static void printBuildIdentifiers(Logger logger) {
        try {
            String columnFormat = "%-35.35s %-35.35s %-20.20s %-30.30s";
            String[] columns = new String[] {"name", "last-modified", "version", "commit"};
            Enumeration<URL> resources = Main.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( "Loaded libraries:\n");
            stringBuilder.append(String.format(Locale.ROOT, columnFormat,
                    (Object[]) columns));
            stringBuilder.append( "\n");
            Map<String, ArrayList<String>> rows = new HashMap<>();
            for (String column : columns) {
                rows.put(column, new ArrayList<>());
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                    "EEE, d MMM yyyy HH:mm:ss Z", Locale.getDefault());
            int nbRows = 0;
            while (resources.hasMoreElements()) {
                try {
                    Manifest manifest = new Manifest(resources.nextElement().openStream());
                    Attributes attributes = manifest.getMainAttributes();
                    String bundleName = attributes.getValue("Bundle-Name");
                    String bundleVersion = attributes.getValue("Bundle-Version");
                    String gitCommitId = attributes.getValue("Implementation-Build");
                    String lastModifier = attributes.getValue("Bnd-LastModified");
                    if(bundleName != null) {
                        nbRows++;
                        rows.get(columns[0]).add(bundleName);
                        if(lastModifier != null) {
                            long lastModifiedLong = Long.parseLong(lastModifier);
                            rows.get(columns[1]).add(simpleDateFormat.format(new Date(lastModifiedLong)));
                        } else {
                            rows.get(columns[1]).add(" - ");
                        }
                        rows.get(columns[2]).add(bundleVersion != null ? bundleVersion : " - ");
                        rows.get(columns[3]).add(gitCommitId != null ? gitCommitId : " - ");
                    }
                } catch (IOException ex) {
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            }
            for(int idRow = 0; idRow < nbRows; idRow++) {
                String[] rowValues = new String[columns.length];
                for (int idColumn = 0; idColumn < columns.length; idColumn++) {
                    String column = columns[idColumn];
                    rowValues[idColumn] = rows.get(column).get(idRow);
                }
                stringBuilder.append(String.format(Locale.ROOT, columnFormat,
                        (Object[]) rowValues));
                stringBuilder.append("\n");
            }
            logger.info(stringBuilder.toString());
        } catch (IOException ex) {
            logger.error("Error while accessing resources", ex);
        }
    }

    public static class ClusterConfiguration {
        public List<String> roads_uueids = new ArrayList<>();
        public List<String> rails_uueids = new ArrayList<>();
    }

    public static ClusterConfiguration loadClusterConfiguration(String workingDirectory, int nodeId) throws IOException {
        // Load Json cluster configuration file
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new File(workingDirectory, "cluster_config.json"));
        JsonNode v;
        ClusterConfiguration configuration = new ClusterConfiguration();
        if (node instanceof ArrayNode) {
            ArrayNode aNode = (ArrayNode) node;
            for (JsonNode cellNode : aNode) {
                JsonNode nodeIdProp = cellNode.get("nodeId");
                if(nodeIdProp != null && nodeIdProp.canConvertToInt() && nodeIdProp.intValue() == nodeId) {
                    if(cellNode.get("roads_uueids") instanceof ArrayNode) {
                        for (JsonNode uueidNode : cellNode.get("roads_uueids")) {
                            configuration.roads_uueids.add(uueidNode.asText());
                        }
                    }
                    if(cellNode.get("rails_uueids") instanceof ArrayNode) {
                        for (JsonNode uueidNode : cellNode.get("rails_uueids")) {
                            configuration.rails_uueids.add(uueidNode.asText());
                        }
                    }
                    break;
                }
            }
        }
        return configuration;
    }

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Main.class.getResource("log4j.properties"));

        Logger logger = LoggerFactory.getLogger("org.noise_planet");

        printBuildIdentifiers(logger);
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
            ClusterConfiguration clusterConfiguration = loadClusterConfiguration(workingDir, nodeId);
            logger.info(String.format(Locale.ROOT, "For job %d, will compute the following UUEID (%s)",
                    nodeId, Stream.concat(clusterConfiguration.roads_uueids.stream(),
                            clusterConfiguration.rails_uueids.stream()).collect(Collectors.joining(","))));
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
                RootProgressVisitor progressVisitor = new RootProgressVisitor(2, true,
                        SECONDS_BETWEEN_PROGRESSION_PRINT);
                nm.uueidsLoop(progressVisitor, clusterConfiguration.roads_uueids, NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_ROAD);
                nm.uueidsLoop(progressVisitor, clusterConfiguration.rails_uueids, NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL);

                // export metadata
                PreparedStatement ps = connection.prepareStatement("CALL CSVWRITE(?, ?)");
                ps.setString(1,new File(nm.outputFolder,
                        nm.outputPrefix + "METADATA.csv").getAbsolutePath() );
                ps.setString(2, "SELECT *, (EXTRACT(EPOCH FROM ROAD_END) - EXTRACT(EPOCH FROM ROAD_START)) ROAD_TOTAL,(EXTRACT(EPOCH FROM GRID_END) - EXTRACT(EPOCH FROM GRID_START)) GRID_TOTAL  FROM METADATA");
                ps.execute();
            } catch (SQLException ex) {
                while (ex != null) {
                    logger.error(ex.getLocalizedMessage(), ex);
                    ex = ex.getNextException();
                }
                System.exit(1);
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            System.exit(1);
        }
    }
}