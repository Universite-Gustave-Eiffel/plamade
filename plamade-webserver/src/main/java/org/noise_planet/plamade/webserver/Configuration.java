/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 * <p>
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Contact: contact@noise-planet.org
 *
 */
package org.noise_planet.plamade.webserver;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.cli.*;
import org.h2gis.functions.factory.H2GISDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Manage webserver configuration
 * Generate datasource configuration from configuration files/args
 */
public class Configuration {
    String scriptPath = "plamade/scripts";
    boolean totpEnabled = false;
    boolean printVersion = false;
    String workingDirectory = System.getProperty("user.home") + "/.noisemodelling";
    // secureBase is the h2 database that store web application critical data
    // it is not associated with any noisemodelling data
    String secureBaseEncryptionSecret = "";
    String secureBaseAdminUser = "sa";
    String secureBaseAdminPassword = "sa";
    Map<String, Object> customConfiguration = new HashMap<String, Object>();

    /**
     * Creates a Configuration object from command-line arguments (backward compatible entry point).
     * Delegates to {@link #createConfigurationFromCommandLine(String[], Options)} using default options.
     *
     * @param args command-line arguments
     * @return Configuration with properties set according to command-line arguments
     * @throws IllegalArgumentException if required options are missing or invalid
     */
    public static Configuration createConfigurationFromArguments(String[] args) throws IllegalArgumentException {
        return createConfigurationFromCommandLine(args, buildOptions());
    }

    /**
     * Build the CLI Options definition used for both CLI and JSON parsing.
     * Required flags defined here are also enforced when parsing from JSON.
     */
    public static Options buildOptions() {
        Options options = new Options();

        Option workingDirOption = new Option("w", "working-dir", true,
                "Path were the application have writing rights to store sessions data");
        workingDirOption.setRequired(false);
        workingDirOption.setArgName("folder path");
        options.addOption(workingDirOption);

        Option scriptPathOption = new Option("s", "script", true, "Path and file name of the script");
        scriptPathOption.setRequired(true);
        scriptPathOption.setArgName("script path");
        options.addOption(scriptPathOption);

        Option printVersionOption = new Option("v", "print-version", false, "Print version of all libraries");
        options.addOption(printVersionOption);

        Option totpEnabledOption = new Option("t", "totp-enabled", false, "Enable TOTP");
        options.addOption(totpEnabledOption);

        Option secureBaseEncryptionSecret = new Option("e", "encryption-secret", true,
                "If provided will encrypt the webserver h2 database with this secret");
        options.addOption(secureBaseEncryptionSecret);

        Option secureBaseAdminPassword = new Option("p", "password-admin", true, "WebServer admin password");
        options.addOption(secureBaseAdminPassword);

        return options;
    }

    /**
     * Creates a Configuration object from command-line arguments using the provided Options definition.
     *
     * @param args command-line arguments
     * @param options Apache Commons CLI options definition
     * @return Configuration configured from CLI
     * @throws IllegalArgumentException if parsing fails
     */
    public static Configuration createConfigurationFromCommandLine(String[] args, Options options)
            throws IllegalArgumentException {
        Configuration config = new Configuration();

        Logger logger = LoggerFactory.getLogger(Configuration.class.getName());
        CommandLineParser commandLineParser = new DefaultParser();
        HelpFormatter helpFormatter = HelpFormatter.builder()
                .setPrintWriter(new PrintWriter(new LoggerWriter(logger))).get();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args, true);
            // Map options to fields
            if (commandLine.hasOption("s")) {
                config.scriptPath = commandLine.getOptionValue("s");
            }
            if (commandLine.hasOption("v")) {
                config.printVersion = true;
            }
            if (commandLine.hasOption("t")) {
                config.totpEnabled = true;
            }
            if (commandLine.hasOption("w")) {
                config.workingDirectory = commandLine.getOptionValue("w");
            }
        } catch (ParseException ex) {
            logger.info(ex.getMessage());
            helpFormatter.printHelp("NoiseModelling Script Runner", options);
        }

        return config;
    }

    /**
     * Creates a Configuration object from a JSON-like document represented as a Map.
     * Keys must use the long option names only (short names are reserved for CLI parsing).
     * Required flags from the provided {@link Options} instance are enforced.
     *
     * Supported keys:
     * - script: String
     * - working-dir: String
     * - totp-enabled: boolean
     * - print-version: boolean
     *
     * @param values  map containing configuration values (e.g., result of JSON parsing)
     * @param options options definition specifying required fields
     * @return Configuration configured from provided values
     * @throws IllegalArgumentException if a required option is missing
     */
    public static Configuration createConfigurationFromJson(Map<String, Object> values, Options options)
            throws IllegalArgumentException {
        Configuration config = new Configuration();

        // Helper to read by long name only (no short names for JSON)
        java.util.function.Function<Option, Object> getValue = (opt) -> {
            String longName = opt.getLongOpt();
            if (longName != null && values.containsKey(longName)) {
                return values.get(longName);
            }
            return null;
        };

        // Enforce required options
        for (Object o : options.getOptions()) {
            Option opt = (Option) o;
            if (opt.isRequired()) {
                Object v = getValue.apply(opt);
                if (v == null) {
                    String name = opt.getLongOpt() != null ? opt.getLongOpt() : opt.getOpt();
                    throw new IllegalArgumentException("Missing required configuration option: " + name);
                }
            }
        }

        // Map known options using long names only
        Object vScript = values.get("script");
        if (vScript instanceof String) {
            config.scriptPath = (String) vScript;
        }
        Object vWork = values.get("working-dir");
        if (vWork instanceof String) {
            config.workingDirectory = (String) vWork;
        }
        Object vTotp = values.get("totp-enabled");
        if (vTotp != null) {
            config.totpEnabled = parseBoolean(vTotp);
        }
        Object vPrint = values.get("print-version");
        if (vPrint != null) {
            config.printVersion = parseBoolean(vPrint);
        }
        Object vSecureBaseEncryptionSecret = values.get("encryption-secret");
        if (vSecureBaseEncryptionSecret != null) {
            config.secureBaseEncryptionSecret = vSecureBaseEncryptionSecret.toString();
        }
        Object vSecureBaseAdminUser = values.get("secure-base-admin-user");
        if (vSecureBaseAdminUser != null) {
            config.secureBaseAdminUser = (String) vSecureBaseAdminUser;
        }
        Object vSecureBaseAdminPassword = values.get("secure-base-admin-password");
        if (vSecureBaseAdminPassword != null) {
            config.secureBaseAdminPassword = (String) vSecureBaseAdminPassword;
        }
        return config;
    }

    private static boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return false;
    }

    public HikariDataSource createWebServerDataSource() throws SQLException {
        HikariConfig config = new HikariConfig();

        StringBuilder connectionUrl = new StringBuilder();
        connectionUrl.append(H2GISDBFactory.START_URL);
        try {
            connectionUrl.append(
                    new File(workingDirectory, "webserver")
                            .toURI()
                            .toURL()
            );
        } catch (Exception e) {
            throw new RuntimeException("Error building H2GIS JDBC URL", e);
        }
        if(!secureBaseEncryptionSecret.isEmpty()) {
            connectionUrl.append(";CIPHER=AES");
        }

        Properties properties = new Properties();
        properties.setProperty(H2GISDBFactory.JDBC_URL, connectionUrl.toString());
        properties.setProperty(
                H2GISDBFactory.JDBC_USER, secureBaseAdminUser);
        properties.setProperty(
                H2GISDBFactory.JDBC_PASSWORD, secureBaseEncryptionSecret.isEmpty() ?
                        secureBaseAdminPassword : secureBaseEncryptionSecret+" "+secureBaseAdminUser);

        javax.sql.DataSource h2DataSource =
                H2GISDBFactory.createDataSource(properties);
        config.setDataSource(h2DataSource);
        return new HikariDataSource(config);
    }

    /**
     * A Writer implementation that redirects output to an SLF4J Logger instance.
     *
     * This class is useful for redirecting output from a Writer to a logging framework.
     * It buffers the output and logs it when the buffer is flushed or closed.
     */
    public static class LoggerWriter extends Writer {

        /**
         * The SLF4J Logger instance to which output will be redirected.
         */
        private final Logger logger;

        /**
         * A buffer to store the output before it is logged.
         */
        private StringBuilder buffer = new StringBuilder();

        /**
         * Constructs a new LoggerWriter instance that redirects output to the specified Logger.
         *
         * @param logger the SLF4J Logger instance to which output will be redirected
         */
        public LoggerWriter(Logger logger) {
            this.logger = logger;
        }

        /**
         * Writes a portion of an array of characters to the buffer.
         *
         * @param cbuf the array of characters to write
         * @param off the offset from which to start writing
         * @param len the number of characters to write
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            buffer.append(cbuf, off, len);
        }

        /**
         * Flushes the buffer and logs its contents to the Logger.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void flush() throws IOException {
            if (buffer.length() > 0) {
                logger.info(buffer.toString());
                buffer = new StringBuilder();
            }
        }

        /**
         * Closes the Writer, but does not perform any actual closing operation.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            // do nothing
        }
    }
}
