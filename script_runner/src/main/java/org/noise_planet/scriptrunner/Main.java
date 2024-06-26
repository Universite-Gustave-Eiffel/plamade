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
package org.noise_planet.scriptrunner;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.log4j.PropertyConfigurator;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.noise_planet.nmcluster.NoiseModellingInstance;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.noise_planet.nmcluster.Main.printBuildIdentifiers;


public class Main {
    public static final int SECONDS_BETWEEN_PROGRESSION_PRINT = 5;

    public static void main(String... args) throws Exception {
        PropertyConfigurator.configure(Main.class.getResource("log4j.properties"));

        Logger logger = LoggerFactory.getLogger("org.noise_planet");

        printBuildIdentifiers(logger);
        try {
            // Read parameters
            String workingDir = "";
            String scriptPath = "";
            boolean runMergeCbs = false;
            Map<String, String> customParameters = new HashMap<>();
            for (int i = 0; args != null && i < args.length; i++) {
                String a = args[i];
                if(a == null) {
                    continue;
                }
                if (a.startsWith("-w")) {
                    workingDir = a.substring(2);
                    if(!(new File(workingDir).exists())) {
                        logger.error(workingDir + " folder does not exists");
                        workingDir = "";
                    }
                } else if (a.startsWith("-s")) {
                    scriptPath = a.substring(2);
                    if(!(new File(scriptPath).exists())) {
                        logger.error(scriptPath + " script does not exists");
                        scriptPath = "";
                    }
                } else if (a.equals("--mergeCBS")) {
                    runMergeCbs = true;
                } else if(a.contains("=")){
                    String key = a.substring(0, a.indexOf("="));
                    String value = a.substring(a.indexOf("=") + 1);
                    customParameters.put(key, value);
                }
            }
            if (workingDir.isEmpty() || scriptPath.isEmpty()) {
                logger.info("Command line arguments :");
                for (String arg : args) {
                    logger.info("Got argument [" + arg + "]");
                }
                String help = "script_runner -wWORKSPACE -sSCRIPT_PATH\n" +
                        "DESCRIPTION\n" +
                        "-wWORKSPACE path where the database is located\n" +
                        "-sSCRIPT_PATH path of the script\n" +
                        "customParameter=thevalue Custom arguments for the script\n" +
                        "--mergeCBS run merge cbs before the script";
                throw new IllegalArgumentException(help);
            }
            // Open database
            DataSource ds = NoiseModellingInstance.createDataSource("", "", new File(workingDir).getAbsolutePath(), "h2gisdb", false);

            RootProgressVisitor progressVisitor = new RootProgressVisitor(1, true,
                    SECONDS_BETWEEN_PROGRESSION_PRINT);

            try (Connection connection = new ConnectionWrapper(ds.getConnection())) {
                if(runMergeCbs) {
                    NoiseModellingInstance.mergeCBS(connection, NoiseModellingInstance.CBS_GRID_SIZE,
                            NoiseModellingInstance.CBS_MAIN_GRID_SIZE,
                            new RootProgressVisitor(1, true,
                                    SECONDS_BETWEEN_PROGRESSION_PRINT));
                }
                GroovyShell shell = new GroovyShell();
                Script receiversGrid= shell.parse(new File(scriptPath));
                Map<String, Object> inputs = new HashMap<>();
                inputs.putAll(customParameters);
                inputs.put("progressVisitor", progressVisitor);
                Object result = receiversGrid.invokeMethod("exec", new Object[] {connection, inputs});
                if(result != null) {
                    logger.info(result.toString());
                }
            } catch (SQLException ex) {
                while (ex != null) {
                    logger.error(ex.getLocalizedMessage(), ex);
                    ex = ex.getNextException();
                }
                System.exit(1);
            }
        } catch (Throwable ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            System.exit(1);
        }
    }
}