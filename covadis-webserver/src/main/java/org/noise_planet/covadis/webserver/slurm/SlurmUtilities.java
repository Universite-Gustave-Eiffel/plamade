/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.slurm;

import org.h2gis.api.ProgressVisitor;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SlurmUtilities {

    public static List<FileAttributes> parseLSCommand(List<String> lines) {
        List<FileAttributes> fileList = new ArrayList<>(Math.max(12, lines.size()));
        for(String line : lines) {
            StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
            if(!stringTokenizer.hasMoreTokens()) {
                continue;
            }
            long fileSize = Long.parseLong(stringTokenizer.nextToken().trim());
            if(!stringTokenizer.hasMoreTokens()) {
                continue;
            }
            String fileName = stringTokenizer.nextToken().trim();
            fileList.add(new FileAttributes(fileName, fileSize));
        }
        return fileList;
    }

    /**
     * Log output of the computing nodes into the logger
     * we have to keep track of how many bytes we have already read in order to not read two times the same log rows
     * we will use the ls command in conjunction with the tail command
     * @param session
     * @param bytesReadInFiles keep track of logged bytes
     */
    public static void logSlurmJobs(SlurmSession session, String remoteJobFolder, Map<String, Long> bytesReadInFiles) {
        Logger logger = session.getLogger();
        try {
            List<String> output = session.runCommand(String.format("find %s/*.out -type f -printf \"%%s,%%f\\n\"", remoteJobFolder), false);
            List<FileAttributes> files = parseLSCommand(output);
            for (FileAttributes file : files) {
                Long alreadyReadBytes = 0L;
                if (bytesReadInFiles.containsKey(file.fileName)) {
                    alreadyReadBytes = bytesReadInFiles.get(file.fileName);
                }
                // check if more bytes can be read
                if(file.fileSize > alreadyReadBytes) {
                    AtomicLong readBytes = new AtomicLong(0L);
                    // the command "tail -c +N" will skip N bytes and read the remaining bytes
                    logger.info("--------" + file.fileName + "--------");
                    session.runCommand(String.format("tail -c +%d %s/%s", alreadyReadBytes, remoteJobFolder ,file.fileName), true, readBytes);
                    bytesReadInFiles.put(file.fileName, alreadyReadBytes + readBytes.get());
                }
            }
        } catch (IOException e) {
            logger.error("Error while reading remote log files", e);
        }
    }

    public static List<SlurmJobStatus> parseSlurmStatus(List<String> commandOutput) {

        List<SlurmJobStatus> slurmJobStatus = new ArrayList<>();

        // 1. Join the lines into one large string to handle multi-line records
        String fullOutput = String.join("\n", commandOutput);

        // 2. Split the string into individual job records.
        String[] jobRecords = fullOutput.split("(?=JobId=(\\d+) ArrayJobId=)");

        // 3. Define patterns for the two fields we need
        // ArrayTaskId=(\d+) -> Captures digits
        // JobState=(\S+)    -> Captures non-whitespace characters
        Pattern taskIdPattern = Pattern.compile("ArrayTaskId=(\\d+)");
        Pattern statePattern = Pattern.compile("JobState=(\\S+)");

        for (String record : jobRecords) {
            // Skip empty strings or noise
            if (record.trim().isEmpty() || !record.contains("JobId=")) {
                continue;
            }

            Matcher taskMatcher = taskIdPattern.matcher(record);
            Matcher stateMatcher = statePattern.matcher(record);

            // 4. Extract values if both are found in this record
            if (taskMatcher.find() && stateMatcher.find()) {
                try {
                    int taskId = Integer.parseInt(taskMatcher.group(1));
                    String status = stateMatcher.group(1);

                    slurmJobStatus.add(new SlurmJobStatus(status, taskId));
                } catch (NumberFormatException e) {
                    // Handle cases where taskId might not be a valid integer
                    // though \d+ regex usually prevents this
                }
            }
        }

        return slurmJobStatus;
    }

}
