/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.utilities;

import com.fasterxml.jackson.core.*;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2gis.api.ProgressVisitor;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class FileUtilities {


    /**
     * Executes the SQL statements contained in one or more gzip-compressed SQL script files
     * (as produced by H2's {@code SCRIPT TO ... COMPRESSION GZIP}) on the given connection,
     * optionally filtered by a {@link QueryFilter}.
     *
     * @param inputFiles      paths of the gzip-compressed SQL dump files; non-existing files are silently skipped
     * @param connection      connection on which the statements are executed
     * @param progressVisitor progress visitor; one step is consumed per input file
     * @param ignoreErrors    if {@code true}, failing statements are logged and execution continues,
     *                        otherwise the first {@link SQLException} is propagated
     * @param logger          logger used for warnings when {@code ignoreErrors} is {@code true}
     * @param queryFilter     decides for each statement whether it is executed
     * @throws IOException  if a dump file cannot be opened, decompressed or read
     * @throws SQLException if a statement fails and {@code ignoreErrors} is {@code false}
     */
    public static void runSqlDump(List<String> inputFiles, Connection connection, ProgressVisitor progressVisitor,
                                  boolean ignoreErrors, Logger logger, QueryFilter queryFilter)
            throws IOException, SQLException {

        ProgressVisitor subProgress = progressVisitor.subProcess(inputFiles.size());

        try (Statement stmt = connection.createStatement()) {
            for (String file : inputFiles) {
                File inputFile = new File(file);
                if (!inputFile.exists()) continue;

                try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inputFile));
                     InputStreamReader isr = new InputStreamReader(gzis, StandardCharsets.UTF_8);
                     BufferedReader reader = new BufferedReader(isr)) {
                    ScriptReader scriptReader = new ScriptReader(reader);
                    scriptReader.setSkipRemarks(true);
                    String statement = scriptReader.readStatement();
                    while (statement != null) {
                        // readStatement() may prefix statements with a line break left over from skipped remarks
                        statement = statement.trim();
                        if(!StringUtils.isWhitespaceOrEmpty(statement) && queryFilter.accept(statement)) {
                            try {
                                stmt.execute(statement);
                            } catch (SQLException ex) {
                                if (ignoreErrors) {
                                    logger.warn(ex.getLocalizedMessage(), ex);
                                } else {
                                    throw ex;
                                }
                            }
                        }
                        statement = scriptReader.readStatement();
                    }
                }
                subProgress.endStep();
            }
        }
    }

    public interface QueryFilter {
        /**
         * @param statement SQL Query
         * @return True to run the query
         */
        boolean accept(String statement);
    }
}
