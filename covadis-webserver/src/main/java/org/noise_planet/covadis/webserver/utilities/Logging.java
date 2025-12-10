package org.noise_planet.covadis.webserver.utilities;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class Logging {

    public static void configureFileLogger(String workingDirectory, String loggingFileName) {
        try {
            // Create rolling file appender
            RollingFileAppender rollingAppender = createRollingFileAppender(workingDirectory, loggingFileName);

            // init stream
            rollingAppender.activateOptions();

            // Configure root logger
            org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
            rootLogger.addAppender(rollingAppender);
        } catch (Exception e) {
            System.err.println("Failed to configure logger: " + e.getMessage());
        }
    }

    @NotNull
    private static RollingFileAppender createRollingFileAppender(String workingDirectory, String loggingFileName) {
        RollingFileAppender rollingAppender = new RollingFileAppender();

        // Configure appender properties
        rollingAppender.setName("rollingFile");
        rollingAppender.setFile(new File(workingDirectory, loggingFileName).getPath());
        rollingAppender.setAppend(true);
        rollingAppender.setMaxBackupIndex(5);
        rollingAppender.setMaximumFileSize(10_000_000);

        // Create and set pattern layout
        PatternLayout layout = new PatternLayout("[%t] %-5p %d{dd MMM HH:mm:ss} - %m%n");
        rollingAppender.setLayout(layout);
        return rollingAppender;
    }
}
