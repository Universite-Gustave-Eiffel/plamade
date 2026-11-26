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
import org.h2gis.api.ProgressVisitor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FileUtilities {

    /**
     * Merges multiple .sql.gz files into a single .sql.gz file.
     * The first file's create table instruction is preserved along with the trailing
     * constraint and index statements (ALTER TABLE ... ADD CONSTRAINT, CREATE INDEX);
     * later files only contribute the data rows (batched multi-line "INSERT INTO"
     * statements) and stop at the first trailing constraint/index statement so that
     * they are not duplicated in the merged script.
     *
     * @param inputFiles List of paths to .sql.gz files
     * @param outputFile The destination .sql.gz file
     * @throws IOException If an I/O error occurs
     */
    public static void mergeSqlFiles(List<String> inputFiles, File outputFile, ProgressVisitor progressVisitor) throws IOException {
        // Use a large buffer for bulk copying
        char[] buffer = new char[32768]; // 32KB buffer

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             OutputStreamWriter osw = new OutputStreamWriter(gzos, StandardCharsets.UTF_8);
             BufferedWriter writer = new BufferedWriter(osw)) {
            ProgressVisitor subProgress = progressVisitor.subProcess(inputFiles.size());
            for (int fileIndex = 0; fileIndex < inputFiles.size(); fileIndex++) {
                File inputFile = new File(inputFiles.get(fileIndex));
                if (!inputFile.exists()) continue;

                try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inputFile));
                     InputStreamReader isr = new InputStreamReader(gzis, StandardCharsets.UTF_8);
                     BufferedReader reader = new BufferedReader(isr)) {

                    String line;
                    boolean dataStarted = false;
                    while ((line = reader.readLine()) != null) {
                        if (fileIndex == 0 && line.startsWith("CREATE CACHED TABLE")) {
                            // 1. First file: keep the whole script (table creation, data,
                            // constraints and indexes) in one fast bulk transfer
                            writer.write(line);
                            writer.write('\n'); // Standardize on Unix newline for SQL
                            int charsRead;
                            while ((charsRead = reader.read(buffer)) != -1) {
                                writer.write(buffer, 0, charsRead);
                            }
                            // Exit the line-by-line loop and proceed to the next file
                            break;
                        }
                        if (fileIndex == 0 && line.startsWith("INSERT INTO")) {
                            // Fallback if the first file has no CREATE CACHED TABLE statement
                            writer.write(line);
                            writer.write('\n');
                            int charsRead;
                            while ((charsRead = reader.read(buffer)) != -1) {
                                writer.write(buffer, 0, charsRead);
                            }
                            break;
                        }
                        if (fileIndex > 0) {
                            // Later files contribute only the data rows. H2 SCRIPT writes them
                            // as batched multi-line INSERT statements: a line starting with
                            // "INSERT INTO ... VALUES" followed by one "(...)" row per line.
                            // The trailing constraint/index statements (ALTER TABLE, CREATE
                            // INDEX) are already provided by the first file, stop at the first
                            // one after the data section.
                            if (line.startsWith("INSERT INTO") || (dataStarted && line.startsWith("("))) {
                                dataStarted = true;
                                writer.write(line);
                                writer.write('\n');
                            } else if (dataStarted && !line.isBlank() && !line.startsWith("--")) {
                                break;
                            }
                        }
                    }
                }
                // Flush the writer after each file to keep the GZIP compression stream moving
                writer.flush();
                subProgress.endStep();
            }
        }
    }

    /**
     * Merge GeoJSON files
     * @param inputFiles Input files to merge
     * @param outputFile Merged output file destination
     * @throws IOException If an I/O error occurs
     */
    public static void mergeGeoJSONFiles(List<String> inputFiles, File outputFile) throws IOException {
        JsonFactory factory = new JsonFactory();

        // Merges multiple GeoJSON files into a single FeatureCollection preserving first CRS
        try (FileOutputStream os = new FileOutputStream(outputFile);
             JsonGenerator gen = factory.createGenerator(os, JsonEncoding.UTF8)) {

            gen.writeStartObject();
            gen.writeStringField("type", "FeatureCollection");

            boolean featuresArrayStarted = false;

            for (int i = 0; i < inputFiles.size(); i++) {
                String fileName = inputFiles.get(i);
                File f = new File(fileName);

                if (!f.exists()) {
                    continue;
                }

                // try-with-resources for the parser of each file
                try (JsonParser parser = factory.createParser(f)) {
                    while (parser.nextToken() != null) {
                        String fieldName = parser.currentName();

                        // 1. Capture CRS only from the FIRST file
                        if (i == 0 && "crs".equals(fieldName)) {
                            gen.writeFieldName("crs");
                            parser.nextToken(); // Move to the start of the CRS object
                            gen.copyCurrentStructure(parser);
                        }

                        // 2. Handle the "features" array
                        else if ("features".equals(fieldName) && parser.currentToken() == JsonToken.START_ARRAY) {
                            // If this is the first file we are processing, open the global features array
                            if (!featuresArrayStarted) {
                                gen.writeArrayFieldStart("features");
                                featuresArrayStarted = true;
                            }

                            // Stream every feature object from the current file into the output
                            while (parser.nextToken() != JsonToken.END_ARRAY) {
                                gen.copyCurrentStructure(parser);
                            }
                            // Stop parsing this file once we finish its features array
                            break;
                        }
                    }
                }
            }

            // Close the features array and the root object
            if (featuresArrayStarted) {
                gen.writeEndArray();
            } else {
                // Fallback if no features were ever found
                gen.writeArrayFieldStart("features");
                gen.writeEndArray();
            }

            gen.writeEndObject();
        }
    }
}
