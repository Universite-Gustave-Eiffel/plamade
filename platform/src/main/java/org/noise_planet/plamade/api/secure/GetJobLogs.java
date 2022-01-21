/*
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user-friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */


/*
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.plamade.api.secure;

import com.google.common.collect.Maps;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetJobLogs implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetJobLogs.class);
    static final int FETCH_NUMBER_OF_LINES = 5000;

    public static List<String> filterByThread(List<String> messages, String threadId) {
        List<String> filtered = new ArrayList<>();
        boolean match = false;
        StringBuilder sb = new StringBuilder();
        for(String line : messages) {
            int firstHook = line.indexOf("[");
            int lastHook = line.indexOf("]");
            if(firstHook > -1 && lastHook > -1 && firstHook < lastHook) {
                String thread = line.substring(firstHook + 1, lastHook);
                match = thread.equals(threadId);
            }
            if(match && sb.length() > 0) {
                filtered.add(sb.toString());
                sb = new StringBuilder();
            }
            if(match) {
                sb.append(line);
            }
        }
        if(sb.length() > 0) {
            filtered.add(sb.toString());
        }
        return filtered;
    }
    /**
     * Equivalent to "tail -n x file" linux command. Retrieve the n last lines from a file
     * @param logFile
     * @param numberOfLines
     * @return
     * @throws IOException
     */
    public static List<String> getLastLines(File logFile, int numberOfLines) throws IOException {
        ArrayList<String> lastLines = new ArrayList<>(Math.max(20, numberOfLines));
        final int buffer = 8192;
        long fileSize = Files.size(logFile.toPath());
        long read = 0;
        long lastCursor = fileSize;
        StringBuilder sb = new StringBuilder(buffer);
        try(RandomAccessFile f = new RandomAccessFile(logFile.getAbsoluteFile(), "r")) {
            while(lastLines.size() < numberOfLines && read < fileSize) {
                long cursor = Math.max(0, fileSize - read - buffer);
                read += buffer;
                f.seek(cursor);
                byte[] b = new byte[(int)(lastCursor - cursor)];
                lastCursor = cursor;
                f.readFully(b);
                sb.insert(0, new String(b));
                // Reverse search of end of line into the string buffer
                int lastEndOfLine = sb.lastIndexOf("\n");
                while (lastEndOfLine != -1 && lastLines.size() < numberOfLines) {
                    if(sb.length() - lastEndOfLine > 1) { // if more data than just line return
                        lastLines.add(0, sb.substring(lastEndOfLine + 1, sb.length()).trim());
                    }
                    sb.delete(lastEndOfLine, sb.length());
                    lastEndOfLine = sb.lastIndexOf("\n");
                }
            }
        }
        return lastLines;
    }

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser != -1) {
                        final Map<String, Object> model = Maps.newHashMap();
                        List<String> rows = getLastLines(new File("application.log"), FETCH_NUMBER_OF_LINES);
                        rows = filterByThread(rows, Thread.currentThread().getName());
                        model.put("rows", rows);
                        model.put("profile", profile);
                        ctx.render(Template.thymeleafTemplate(model, "joblogs"));
                    }
                });
            } else {
                ctx.render(Template.thymeleafTemplate("blank"));
            }
        });
    }
}
