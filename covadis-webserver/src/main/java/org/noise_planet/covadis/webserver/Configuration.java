/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage webserver configuration
 */
public class Configuration {
    public static final int DEFAULT_PORT = 8000;
    public static final String DEFAULT_APPLICATION_URL = "nmcovadis";
    String applicationRootUrl = DEFAULT_APPLICATION_URL;
    String scriptPath = "scripts";
    boolean unsecure = false;
    String workingDirectory = System.getProperty("user.home") + "/.noisemodelling";
    // secureBase is the h2 database that store web application critical data
    // it is not associated with any noisemodelling data
    String secureBaseEncryptionSecret = "";
    String secureBaseAdminUser = "sa";
    String secureBaseAdminPassword = "sa";
    int port = DEFAULT_PORT;
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
        workingDirOption.setArgName("folder path");
        workingDirOption.setOptionalArg(false);
        options.addOption(workingDirOption);

        Option scriptPathOption = new Option("s", "script", true, "Path and file name of the script");
        scriptPathOption.setArgName("script path");
        options.addOption(scriptPathOption);

        Option unsecureOption = new Option("u", "unsecure", false, "Disable TOTP, visitors can run any process");
        options.addOption(unsecureOption);

        Option secureBaseEncryptionSecret = new Option("e", "encryption-secret", true,
                "If provided will encrypt the webserver h2 database with this secret");
        options.addOption(secureBaseEncryptionSecret);

        Option applicationRootUrlOption = new Option("r", "root-url", true, "Custom root URL for the web application (default " + DEFAULT_APPLICATION_URL+ " )");
        options.addOption(applicationRootUrlOption);

        Option portOption = new Option("p", "port", true, "Server http serve port (default " + DEFAULT_PORT+ " )");
        portOption.setType(Integer.class);
        options.addOption(portOption);

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
            if (commandLine.hasOption("u")) {
                config.unsecure = true;
            }
            if (commandLine.hasOption("w")) {
                config.workingDirectory = commandLine.getOptionValue("w");
            }
            if (commandLine.hasOption("e")) {
                config.secureBaseEncryptionSecret = commandLine.getOptionValue("e");
            }
            if (commandLine.hasOption("r")) {
                config.applicationRootUrl = commandLine.getOptionValue("r");
            }
            if (commandLine.hasOption("p")) {
                config.port = Integer.parseInt(commandLine.getOptionValue("p"));
            }
        } catch (ParseException ex) {
            helpFormatter.printHelp("NoiseModelling Script Runner", options);
            throw new IllegalArgumentException(ex.getMessage());
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
     * - unsecure: boolean
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
        Object vUnsecure = values.get("unsecure");
        if (vUnsecure != null) {
            config.unsecure = parseBoolean(vUnsecure);
        }
        Object vSecureBaseEncryptionSecret = values.remove("encryption-secret");
        if (vSecureBaseEncryptionSecret != null) {
            config.secureBaseEncryptionSecret = vSecureBaseEncryptionSecret.toString();
        }
        Object vApplicationRootUrl = values.get("root-url");
        if (vApplicationRootUrl instanceof String) {
            config.applicationRootUrl = (String) vApplicationRootUrl;
        }
        Object vPort = values.get("port");
        if (vPort instanceof Integer || vPort instanceof Long) {
            config.port = (int) vPort;
        }
        config.customConfiguration = values;
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

    /**
     * Returns the application root URL.
     *
     * @return the application root URL
     */
    public String getApplicationRootUrl() {
        return applicationRootUrl;
    }

    /**
     * Sets the application root URL.
     *
     * @param applicationRootUrl the application root URL to set
     */
    public void setApplicationRootUrl(String applicationRootUrl) {
        this.applicationRootUrl = applicationRootUrl;
    }

    /**
     * Returns the script path.
     *
     * @return the script path
     */
    public String getScriptPath() {
        return scriptPath;
    }

    /**
     * Sets the script path.
     *
     * @param scriptPath the script path to set
     */
    public void setScriptPath(String scriptPath) {
        this.scriptPath = scriptPath;
    }

    /**
     * Returns whether the server is in unsecure mode.
     *
     * @return true if unsecure mode is enabled, false otherwise
     */
    public boolean isUnsecure() {
        return unsecure;
    }

    /**
     * Sets the unsecure mode flag.
     *
     * @param unsecure true to enable unsecure mode, false to disable
     */
    public void setUnsecure(boolean unsecure) {
        this.unsecure = unsecure;
    }

    /**
     * Returns the working directory path.
     *
     * @return the working directory
     */
    public String getWorkingDirectory() {
        return workingDirectory;
    }

    /**
     * Sets the working directory path.
     *
     * @param workingDirectory the working directory to set
     */
    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    /**
     * Returns the encryption secret for the secure base.
     *
     * @return the secure base encryption secret
     */
    public String getSecureBaseEncryptionSecret() {
        return secureBaseEncryptionSecret;
    }

    /**
     * Sets the encryption secret for the secure base.
     *
     * @param secureBaseEncryptionSecret the encryption secret to set
     */
    public void setSecureBaseEncryptionSecret(String secureBaseEncryptionSecret) {
        this.secureBaseEncryptionSecret = secureBaseEncryptionSecret;
    }

    /**
     * Returns the secure base admin user.
     *
     * @return the secure base admin user
     */
    public String getSecureBaseAdminUser() {
        return secureBaseAdminUser;
    }

    /**
     * Sets the secure base admin user.
     *
     * @param secureBaseAdminUser the admin user to set
     */
    public void setSecureBaseAdminUser(String secureBaseAdminUser) {
        this.secureBaseAdminUser = secureBaseAdminUser;
    }

    /**
     * Returns the secure base admin password.
     *
     * @return the secure base admin password
     */
    public String getSecureBaseAdminPassword() {
        return secureBaseAdminPassword;
    }

    /**
     * Sets the secure base admin password.
     *
     * @param secureBaseAdminPassword the admin password to set
     */
    public void setSecureBaseAdminPassword(String secureBaseAdminPassword) {
        this.secureBaseAdminPassword = secureBaseAdminPassword;
    }

    /**
     * Returns the custom configuration map.
     *
     * @return the custom configuration
     */
    public Map<String, Object> getCustomConfiguration() {
        return customConfiguration;
    }

    /**
     * Sets the custom configuration map.
     *
     * @param customConfiguration the custom configuration to set
     */
    public void setCustomConfiguration(Map<String, Object> customConfiguration) {
        this.customConfiguration = customConfiguration;
    }

    /**
     * @return Server port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port Server port
     */
    public void setPort(int port) {
        this.port = port;
    }
}
