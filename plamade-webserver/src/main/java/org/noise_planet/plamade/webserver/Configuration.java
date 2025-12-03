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

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Configuration {
    String scriptPath = "plamade/scripts";
    boolean totpEnabled = false;
    boolean printVersion = false;
    String workingDirectory = System.getProperty("user.home") + "/.noisemodelling";

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

        Option printVersionOption = new Option("v", null, false, "Print version of all libraries");
        options.addOption(printVersionOption);

        Option totpEnabledOption = new Option("t", "totp-enabled", false, "Enable TOTP");
        options.addOption(totpEnabledOption);

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
     * Keys can be either the long option name (when available) or the short option name.
     * Required flags from the provided {@link Options} instance are enforced.
     *
     * Supported keys:
     * - script / s: String
     * - working-dir / w: String
     * - totp-enabled / t: boolean
     * - v: boolean (print version)
     *
     * @param values  map containing configuration values (e.g., result of JSON parsing)
     * @param options options definition specifying required fields
     * @return Configuration configured from provided values
     * @throws IllegalArgumentException if a required option is missing
     */
    public static Configuration createConfigurationFromJson(java.util.Map<String, Object> values, Options options)
            throws IllegalArgumentException {
        Configuration config = new Configuration();

        // Helper to read by long name first, then short name
        java.util.function.Function<Option, Object> getValue = (opt) -> {
            String longName = opt.getLongOpt();
            String shortName = opt.getOpt();
            if (longName != null && values.containsKey(longName)) {
                return values.get(longName);
            }
            if (shortName != null && values.containsKey(shortName)) {
                return values.get(shortName);
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

        // Map known options
        Object vScript = getValue.apply(options.getOption("s"));
        if (vScript instanceof String) {
            config.scriptPath = (String) vScript;
        }
        Object vWork = getValue.apply(options.getOption("w"));
        if (vWork instanceof String) {
            config.workingDirectory = (String) vWork;
        }
        Object vTotp = getValue.apply(options.getOption("t"));
        if (vTotp != null) {
            config.totpEnabled = parseBoolean(vTotp);
        }
        // print version has only short opt 'v'
        Object vPrint = getValue.apply(options.getOption("v"));
        if (vPrint != null) {
            config.printVersion = parseBoolean(vPrint);
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
