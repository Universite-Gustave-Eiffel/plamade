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

import java.io.BufferedReader;
import java.io.File;

import static org.h2.server.web.PageParser.escapeHtml;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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


    public static List<String> getAllLines(String jobId, int numberOfLines) throws IOException {
        List<File> logFiles = new ArrayList<>();
        logFiles.add(new File("application.log"));
        int logCounter = 1;
        while(true) {
            File oldLogFile = new File("application.log." + (logCounter++));
            if(oldLogFile.exists()) {
                logFiles.add(oldLogFile);
            } else {
                break;
            }
        }
        List<String> rows = new ArrayList<>(numberOfLines == -1 ? 1000 : numberOfLines);
        for(File logFile : logFiles) {
            rows.addAll(0, NoiseModellingRunner.getLastLines(logFile,
                    numberOfLines == -1 ? -1 : numberOfLines - rows.size(),
                    String.format("JOB_%s", jobId)));
            if(numberOfLines != -1 && rows.size() >= numberOfLines) {
                break;
            }
        }
        return rows;
    }

    /**
     * Equivalent to "tail -n x file" linux command. Retrieve the n last lines from a file
     * @param logFile
     * @param numberOfLines
     * @return
     * @throws IOException
     */
    public static List<String> getLastLines(File logFile, int numberOfLines, String threadId) throws IOException {
        boolean match = threadId.isEmpty();
        StringBuilder sbMatch = new StringBuilder();
        ArrayList<String> lastLines = new ArrayList<>(Math.max(20, numberOfLines));
        final int buffer = 8192;
        long fileSize = Files.size(logFile.toPath());
        long read = 0;
        long lastCursor = fileSize;
        StringBuilder sb = new StringBuilder(buffer);
        try(RandomAccessFile f = new RandomAccessFile(logFile.getAbsoluteFile(), "r")) {
            while((numberOfLines == -1 || lastLines.size() < numberOfLines) && read < fileSize) {
                long cursor = Math.max(0, fileSize - read - buffer);
                read += buffer;
                f.seek(cursor);
                byte[] b = new byte[(int)(lastCursor - cursor)];
                lastCursor = cursor;
                f.readFully(b);
                sb.insert(0, new String(b));
                // Reverse search of end of line into the string buffer
                int lastEndOfLine = sb.lastIndexOf("\n");
                while (lastEndOfLine != -1 && (numberOfLines == -1 || lastLines.size() < numberOfLines)) {
                    if(sb.length() - lastEndOfLine > 1) { // if more data than just line return
                        String line = sb.substring(lastEndOfLine + 1, sb.length()).trim();
                        if(!threadId.isEmpty()) {
                            int firstHook = line.indexOf("[");
                            int lastHook = line.indexOf("]");
                            if (firstHook == 0 && firstHook < lastHook) {
                                String thread = line.substring(firstHook + 1, lastHook);
                                match = thread.equals(threadId);
                            }
                        }
                        if (match && sbMatch.length() > 0) {
                            lastLines.add(0, sbMatch.toString());
                            sbMatch = new StringBuilder();
                        }
                        if(match) {
                            sbMatch.append(line);
                        }
                    }
                    sb.delete(lastEndOfLine, sb.length());
                    lastEndOfLine = sb.lastIndexOf("\n");
                }
            }
        }
        return lastLines;
    }

    private static boolean matchesThreadId(String line, String threadId) {
        if (threadId.isEmpty()) {
            return true;
        } else {
            int firstHook = line.indexOf("[");
            int lastHook = line.indexOf("]");
            if (firstHook == 0 && firstHook < lastHook) {
                String thread = line.substring(firstHook + 1, lastHook);
                return thread.equals(threadId);
            } else {
                return false;
            }
        }
    }

}
