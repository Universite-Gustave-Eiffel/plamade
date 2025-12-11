/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.covadis.webserver.utilities;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.jetbrains.annotations.NotNull;

import java.io.File;

import static org.h2.server.web.PageParser.escapeHtml;

/**
 * Utility functions related to logging features
 */
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

    /**
     * Build an HTML-friendly stack trace string similar to what SLF4J would print,
     * including the exception type, message, stack frames, causes, and suppressed exceptions.
     */
    public static String formatThrowableAsHtml(Throwable throwable) {
        if (throwable == null) return "";
        StringBuilder sb = new StringBuilder();

        // Detect circular references
        java.util.IdentityHashMap<Throwable, Boolean> seen = new java.util.IdentityHashMap<>();

        Throwable t = throwable;
        String prefix = "";
        while (t != null && !seen.containsKey(t)) {
            seen.put(t, Boolean.TRUE);

            // Exception header (class: message)
            String header = t.getClass().getName();
            String msg = t.getMessage();
            if (msg != null && !msg.isEmpty()) {
                header += ": " + msg;
            }
            sb.append(escapeHtml(prefix + header)).append("<br>");

            // Stack frames
            for (StackTraceElement el : t.getStackTrace()) {
                sb.append(escapeHtml(prefix + "\tat " + el)).append("<br>");
            }

            // Suppressed exceptions
            for (Throwable sup : t.getSuppressed()) {
                appendSuppressed(sb, sup, seen, prefix + "\t");
            }

            // Move to cause
            t = t.getCause();
            if (t != null && !seen.containsKey(t)) {
                sb.append(escapeHtml(prefix + "Caused by: ")).append("<br>");
            }
        }

        return sb.toString();
    }

    private static void appendSuppressed(StringBuilder sb, Throwable sup, java.util.IdentityHashMap<Throwable, Boolean> seen, String prefix) {
        if (sup == null || seen.containsKey(sup)) return;
        seen.put(sup, Boolean.TRUE);

        String header = sup.getClass().getName();
        String msg = sup.getMessage();
        if (msg != null && !msg.isEmpty()) {
            header += ": " + msg;
        }
        sb.append(escapeHtml(prefix + "Suppressed: " + header)).append("<br>");
        for (StackTraceElement el : sup.getStackTrace()) {
            sb.append(escapeHtml(prefix + "\tat " + el)).append("<br>");
        }
        for (Throwable nested : sup.getSuppressed()) {
            appendSuppressed(sb, nested, seen, prefix + "\t");
        }
        if (sup.getCause() != null) {
            sb.append(escapeHtml(prefix + "Caused by: ")).append("<br>");
            appendSuppressed(sb, sup.getCause(), seen, prefix + "\t");
        }
    }
}
