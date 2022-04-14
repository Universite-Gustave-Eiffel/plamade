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


package org.noise_planet.plamade.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.sql.Sql;
import org.apache.commons.compress.utils.CountingInputStream;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.functions.io.geojson.GeoJsonWrite;
import org.h2gis.functions.spatial.mesh.ST_Tessellate;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.operation.overlay.OverlayOp;
import org.noise_planet.nmcluster.Main;
import org.noise_planet.nmcluster.NoiseModellingInstance;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.utils.PowerUtils;
import org.noise_planet.plamade.config.DataBaseConfig;
import org.noise_planet.plamade.config.SlurmConfig;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This process hold one instance of NoiseModelling
 * @author Nicolas Fortin, Université Gustave Eiffel
 */
public class NoiseModellingRunner implements RunnableFuture<String> {

    public static final String MAIN_JOBS_FOLDER = "jobs_running";
    public static final String JOB_LOG_FILENAME = "joblog.txt";
    private static final int BATCH_MAX_SIZE = 100;
    public static final String H2GIS_DATABASE_NAME = "h2gisdb";
    public enum JOB_STATES {
        QUEUED,
        RUNNING,
        FAILED,
        CANCELED,
        COMPLETED
    }
     public static final int MAX_CONNECTION_RETRY = 170;
    public static final int MAX_SSH_SEND_RETRY = 4;
    public static final int CBS_GRID_SIZE = 10;
    public static final int CBS_MAIN_GRID_SIZE = 800;
    public static final String RESULT_DIRECTORY_NAME = "results";
    public static final String POST_PROCESS_RESULT_DIRECTORY_NAME = "results_post";

    // https://curc.readthedocs.io/en/latest/running-jobs/squeue-status-codes.html
    public static final SlurmJobKnownStatus[] SLURM_JOB_KNOWN_STATUSES = new SlurmJobKnownStatus[]{
            new SlurmJobKnownStatus("COMPLETED", true, false), // The job has completed successfully.
            new SlurmJobKnownStatus("COMPLETING", false, false), // The job is finishing but some processes are still active.
            new SlurmJobKnownStatus("FAILED", true, true), // The job terminated with a non-zero exit code and failed to execute.
            new SlurmJobKnownStatus("PENDING", false, false), // The job is waiting for resource allocation. It will eventually run.
            new SlurmJobKnownStatus("PREEMPTED", false, false), // The job was terminated because of preemption by another job.
            new SlurmJobKnownStatus("RUNNING", false, false), // The job currently is allocated to a node and is running.
            new SlurmJobKnownStatus("SUSPENDED", false, false), // A running job has been stopped with its cores released to other jobs.
            new SlurmJobKnownStatus("STOPPED", true, false), // A running job has been stopped with its cores retained.
            new SlurmJobKnownStatus("CANCELED", true, true), // Job canceled by system or user
            new SlurmJobKnownStatus("TIMEOUT", true, true) // Job timeout (will not be restarted)
    };

    private static final Logger logger = LoggerFactory.getLogger(NoiseModellingRunner.class);
    Configuration configuration;
    DataSource nmDataSource;
    DataSource plamadeDataSource;
    boolean isRunning = false;
    private static final int SFTP_TIMEOUT = 60000;
    private static final int POLL_SLURM_STATUS_TIME = 40000;

    private static final String BATCH_FILE_NAME = "noisemodelling_batch.sh";
    private int oldFinishedJobs = 0;
    private Map<String, SlurmJobKnownStatus> slurmStateMap = new TreeMap<>();

    public NoiseModellingRunner(Configuration configuration, DataSource plamadeDataSource) {
        this.configuration = configuration;
        this.plamadeDataSource = plamadeDataSource;
        // Loop check for job status
        for(SlurmJobKnownStatus s : SLURM_JOB_KNOWN_STATUSES) {
            slurmStateMap.put(s.status, s);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static DataSource createDataSource(String user, String password, String dbDirectory, String dbName, boolean debug) throws SQLException {
        // Create H2 memory DataSource
        org.h2.Driver driver = org.h2.Driver.load();
        OsgiDataSourceFactory dataSourceFactory = new OsgiDataSourceFactory(driver);
        Properties properties = new Properties();
        String databasePath = "jdbc:h2:" + new File(dbDirectory, dbName).getAbsolutePath();
        properties.setProperty(DataSourceFactory.JDBC_URL, databasePath);
        properties.setProperty(DataSourceFactory.JDBC_USER, user);
        properties.setProperty(DataSourceFactory.JDBC_PASSWORD, password);
        if(debug) {
            properties.setProperty("TRACE_LEVEL_FILE", "3"); // enable debug
        }
        DataSource dataSource = dataSourceFactory.createDataSource(properties);
        // Init spatial ext
        try (Connection connection = dataSource.getConnection()) {
            if(!JDBCUtilities.tableExists(connection, "GEOMETRY_COLUMNS")) {
                H2GISFunctions.load(connection);
            }
        }
        return dataSource;

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

    public void importData(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {

        GroovyShell shell = new GroovyShell();
        Script extractDepartment= shell.parse(new File("../script_groovy", "s1_Extract_Department.groovy"));

        Map<String, Object> inputs = new HashMap<>();
        inputs.put("databaseUser", configuration.getDataBaseConfig().user);
        inputs.put("databasePassword", configuration.getDataBaseConfig().password);
        inputs.put("fetchDistance", 1000);
        inputs.put("inseeDepartment", configuration.getInseeDepartment());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("inputServer", "cloud");

        Object result = extractDepartment.invokeMethod("exec", new Object[] {nmConnection, inputs});

        if(result instanceof String) {
            try (OutputStreamWriter f = new OutputStreamWriter(new FileOutputStream(new File(configuration.getWorkingDirectory(), "import.html")), StandardCharsets.UTF_8)) {
                f.write(result.toString());
            }
        }
    }

    public static void makeGrid(Connection nmConnection, int configurationId) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script receiversGrid= shell.parse(new File("../script_groovy", "s2_Receivers_Grid.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configurationId);

        Object result = receiversGrid.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public void makeEmission(Connection nmConnection) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script receiversGrid= shell.parse(new File("../script_groovy", "s3_Emission_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());

        Object result = receiversGrid.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public static JsonNode convertToJson(List<ArrayList<PointNoiseMap.CellIndex>> cells) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootNode = mapper.createArrayNode();
        int nodeIndex = 0;
        for(ArrayList<PointNoiseMap.CellIndex> node : cells) {
            for(PointNoiseMap.CellIndex cellIndex : node) {
                ObjectNode cellNode = mapper.createObjectNode();
                cellNode.put("nodeIndex", nodeIndex);
                cellNode.put("latitudeIndex", cellIndex.getLatitudeIndex());
                cellNode.put("longitudeIndex", cellIndex.getLongitudeIndex());
                rootNode.add(cellNode);
            }
            nodeIndex++;
        }
        return rootNode;
    }

    private static class JobElement implements Comparable<JobElement> {
        public List<String> roadsUueid = new ArrayList<>();
        public List<String> railsUueid = new ArrayList<>();
        public double totalSourceLineLength = 0;


        @Override
        public int compareTo(JobElement o) {
            return Double.compare(o.totalSourceLineLength, totalSourceLineLength);
        }
    }

    public static Integer asInteger(Object v) {
        if(v instanceof Number) {
            return ((Number)v).intValue();
        } else {
            return null;
        }
    }

    public static class CSVFileFilter implements FilenameFilter {
        String prefix;
        String suffix;

        public CSVFileFilter(String prefix, String suffix) {
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public boolean accept(File dir, String name) {
            if(!name.toLowerCase(Locale.ROOT).endsWith("csv")) {
                return false;
            }
            if(!name.startsWith(prefix)) {
                return false;
            }
            return suffix.isEmpty() || name.indexOf(suffix, prefix.length()) != -1;
        }
    }
    public static class GEOJSONFileFilter implements FilenameFilter {
        String prefix;
        String suffix;

        public GEOJSONFileFilter(String prefix, String suffix) {
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public boolean accept(File dir, String name) {
            if(!name.toLowerCase(Locale.ROOT).endsWith("geojson")) {
                return false;
            }
            if(!name.startsWith(prefix)) {
                return false;
            }
            return suffix.isEmpty() || name.indexOf(suffix, prefix.length()) != -1;
        }
    }

    private static class CbsSplitedEntry {
        double noiseLevel;
        Geometry geometry;

        public CbsSplitedEntry(double noiseLevel, Geometry geometry) {
            this.noiseLevel = noiseLevel;
            this.geometry = geometry;
        }
    }

    private static class IsoEntry {
        int index;
        Geometry cell;
        String isoLvl;

        public IsoEntry(int index, Geometry cell, String isoLvl) {
            this.index = index;
            this.cell = cell;
            this.isoLvl = isoLvl;
        }
    }
    /**
     * Merge UUEID in CBS over a regular grid
     * @param connection
     * @return
     */
    public static List<String> mergeCBS(Connection connection, int gridSize, int mainGridSize, ProgressVisitor progressVisitor) throws SQLException {
        List<String> allTables = JDBCUtilities.getTableNames(connection, null, null, null,
                new String[]{"TABLE"});
        GeometryFactory geometyFactory = new GeometryFactory();
        Map<String, Double> isoLabelToLevel = new TreeMap<>();
        isoLabelToLevel.put("Lden5559", 57.0);
        isoLabelToLevel.put("Lden6064", 62.0);
        isoLabelToLevel.put("Lden6569", 67.0);
        isoLabelToLevel.put("Lden7074", 72.0);
        isoLabelToLevel.put("LdenGreaterThan75", 75.0);
        isoLabelToLevel.put("Lnight5054", 52.0);
        isoLabelToLevel.put("Lnight5559", 57.0);
        isoLabelToLevel.put("Lnight6064", 62.0);
        isoLabelToLevel.put("Lnight6569", 67.0);
        isoLabelToLevel.put("LnightGreaterThan70", 70.0);
        ArrayList<String> outputTables = new ArrayList<>();
        ArrayList<TableLocation> cbsTables = new ArrayList<>();
        for(String tableName : allTables) {
            TableLocation tableLocation = TableLocation.parse(tableName, DBTypes.H2GIS);
            if (tableLocation.getTable().startsWith("CBS_A_R_") || tableLocation.getTable().startsWith("CBS_A_F_")) {
                List<String> fields = JDBCUtilities.getColumnNames(connection, tableLocation);
                if (!(fields.contains("UUEID") && fields.contains("PERIOD") && fields.contains("NOISELEVEL"))) {
                    continue;
                }
                cbsTables.add(tableLocation);
            }
        }
        ProgressVisitor tableProgress = progressVisitor.subProcess(cbsTables.size());
        for(TableLocation tableLocation : cbsTables) {
            String outputTable = tableLocation.getTable() + "_MERGED";
            try(Statement st = connection.createStatement()) {
                final STRtree tesselatedIsos = new STRtree();
                Envelope tableExtent = GeometryTableUtilities.getEstimatedExtent(connection, tableLocation).getEnvelopeInternal();
                double minX = tableExtent.getMinX();
                double minY = tableExtent.getMinY();
                double cellHeight = tableExtent.getHeight();
                int maxJ = (int)Math.ceil(cellHeight / gridSize);
                Map<PointNoiseMap.CellIndex, ArrayList<PointNoiseMap.CellIndex>> cellIndices = new HashMap<>();
                long startMerge = System.currentTimeMillis();
                final int indexFactor = mainGridSize / gridSize;
                try(ResultSet rs = st.executeQuery("SELECT THE_GEOM, NOISELEVEL FROM " + tableLocation)) {
                    while (rs.next()) {
                        Geometry inputGeometry = (Geometry) rs.getObject(1);
                        Double noiseLevel = isoLabelToLevel.get(rs.getString(2));
                        for (int idGeometry = 0; idGeometry < inputGeometry.getNumGeometries(); idGeometry++) {
                            Geometry geom = inputGeometry.getGeometryN(idGeometry);
                            Geometry multiPolygon;
                            Envelope geomEnvelope = geom.getEnvelopeInternal();
                            if (geomEnvelope.getWidth() > gridSize || geomEnvelope.getHeight() > gridSize) {
                                // tessellate the geometry if it is larger than the cell of the grid
                                try {
                                    multiPolygon = ST_Tessellate.tessellate(geom);
                                } catch (RuntimeException ex) {
                                    logger.warn("Error while tessellating the polygon", ex);
                                    multiPolygon = geom;
                                }
                            } else {
                                multiPolygon = geom;
                            }
                            for (int idPoly = 0; idPoly < multiPolygon.getNumGeometries(); idPoly++) {
                                Geometry triangle = multiPolygon.getGeometryN(idPoly);
                                Envelope triEnv = triangle.getEnvelopeInternal();
                                int startI = (int) ((triEnv.getMinX() - minX) / gridSize);
                                int startJ = (int) ((triEnv.getMinY() - minY) / gridSize);
                                int endI = (int) Math.ceil((triEnv.getMaxX() - minX) / gridSize);
                                int endJ = (int) Math.ceil((triEnv.getMaxY() - minY) / gridSize);
                                for (int i = startI; i < endI; i++) {
                                    for (int j = startJ; j < endJ; j++) {
                                        int mainI = i / indexFactor;
                                        int mainJ = j / indexFactor;
                                        PointNoiseMap.CellIndex cell = new PointNoiseMap.CellIndex(i, j);
                                        PointNoiseMap.CellIndex key = new PointNoiseMap.CellIndex(mainI, mainJ);
                                        ArrayList<PointNoiseMap.CellIndex> ar = cellIndices.computeIfAbsent(key,
                                                k -> new ArrayList<>());
                                        ar.add(cell);
                                    }
                                }
                                CbsSplitedEntry cbsSplitedEntry = new CbsSplitedEntry(noiseLevel, triangle);
                                tesselatedIsos.insert(triEnv, cbsSplitedEntry);
                            }
                        }
                    }
                }
                tesselatedIsos.build();
                // Iterate over grid
                int srid = GeometryTableUtilities.getSRID(connection, tableLocation);
                st.execute("DROP TABLE IF EXISTS " + outputTable);
                st.execute("CREATE TABLE "+outputTable+"(ID INTEGER, THE_GEOM GEOMETRY(MULTIPOLYGON,"+srid+"), NOISELEVEL VARCHAR(20))");

                boolean isDay = tableLocation.getTable().contains("_LD_");
                long endCells = System.currentTimeMillis();
                logger.info(String.format(Locale.ROOT, "Collect cells in %d ms may generate up to" +
                                " %d iso-cells. Compute intersection of iso-cells..",(int)(endCells - startMerge) ,
                        cellIndices.values().stream().mapToInt(ArrayList::size).sum()));

                Deque<IsoEntry> deque = new ConcurrentLinkedDeque<>();
                ProgressVisitor mainGridProgress = tableProgress.subProcess(cellIndices.size());
                cellIndices.entrySet().parallelStream().forEach(mainGridEntry -> {
                    Map<String, ArrayList<IsoEntry>> mainCellsPolys = new HashMap<>();
                    mainGridEntry.getValue().forEach(cellIndex -> {
                        double x1 = minX + (double) cellIndex.getLongitudeIndex() * gridSize;
                        double y1 = minY + (double) cellIndex.getLatitudeIndex() * gridSize;
                        double x2 = minX + (double) (cellIndex.getLongitudeIndex() + 1) * gridSize;
                        double y2 = minY + (double) (cellIndex.getLatitudeIndex() + 1) * gridSize;
                        Polygon gridCell = geometyFactory.createPolygon(new Coordinate[]{new Coordinate(x1, y1), new Coordinate(x2, y1), new Coordinate(x2, y2), new Coordinate(x1, y2), new Coordinate(x1, y1)});
                        gridCell.setSRID(srid);
                        int cellId = cellIndex.getLongitudeIndex() * maxJ + cellIndex.getLatitudeIndex();
                        double area = gridCell.getArea();
                        // Fetch all polygons that intersects this
                        List<CbsSplitedEntry> results = (List<CbsSplitedEntry>) tesselatedIsos.query(gridCell.getEnvelopeInternal());
                        double sumPower = 0;
                        for (CbsSplitedEntry result : results) {
                            OverlayOp overlayOp = new OverlayOp(gridCell, result.geometry);
                            Geometry geom = overlayOp.getResultGeometry(OverlayOp.INTERSECTION);
                            if (!geom.isEmpty()) {
                                double intersectionArea = geom.getArea();
                                sumPower += PowerUtils.dbaToW(result.noiseLevel) * (intersectionArea / area);
                            }
                        }
                        if (sumPower > 0) {
                            // insert entry
                            double summedNoiseLevel = PowerUtils.wToDba(sumPower);
                            String noiseLevel = "";
                            if (isDay) {
                                if (summedNoiseLevel < 60) {
                                    noiseLevel = "Lden5559";
                                } else if (summedNoiseLevel < 65) {
                                    noiseLevel = "Lden6064";

                                } else if (summedNoiseLevel < 70) {
                                    noiseLevel = "Lden6569";

                                } else if (summedNoiseLevel < 75) {
                                    noiseLevel = "Lden7074";

                                } else {
                                    noiseLevel = "LdenGreaterThan75";
                                }
                            } else {
                                if (summedNoiseLevel < 55) {
                                    noiseLevel = "Lnight5054";
                                } else if (summedNoiseLevel < 60) {
                                    noiseLevel = "Lnight5559";

                                } else if (summedNoiseLevel < 65) {
                                    noiseLevel = "Lnight6064";

                                } else if (summedNoiseLevel < 70) {
                                    noiseLevel = "Lnight6569";
                                } else {
                                    noiseLevel = "LnightGreaterThan70";
                                }
                            }
                            IsoEntry isoEntry = new IsoEntry(cellId, gridCell, noiseLevel);
                            ArrayList<IsoEntry> isoList = mainCellsPolys.putIfAbsent(noiseLevel,
                                    new ArrayList<>(Collections.singleton(isoEntry)));
                            if (isoList != null) {
                                isoList.add(isoEntry);
                            }
                        }
                    });
                    mainCellsPolys.forEach((isoLevel, isoEntries) -> {
                        List<Polygon> multiPoly = new ArrayList<>(isoEntries.size());
                        for(IsoEntry isoEntry : isoEntries) {
                            Geometry cell = isoEntry.cell;
                            for(int idGeom = 0; idGeom < cell.getNumGeometries(); idGeom++) {
                                Geometry subGeom = cell.getGeometryN(idGeom);
                                if(subGeom instanceof Polygon) {
                                    multiPoly.add((Polygon) subGeom);
                                }
                            }
                        }
                        // The best situation for using buffer(0) is the trivial case where there is no overlap between the input geometries.
                        // this is the case here
                        Geometry unionGeom = geometyFactory.createMultiPolygon(multiPoly.toArray(new Polygon[0])).buffer(0);
                        int mainIndex = (maxJ / indexFactor) * mainGridEntry.getKey().getLongitudeIndex() +
                                mainGridEntry.getKey().getLatitudeIndex();
                        deque.add(new IsoEntry(mainIndex, unionGeom, isoLevel));

                    });
                    mainGridProgress.endStep();
                });
                // merge sub cells geometries into main cells
                long endOfIntersects = System.currentTimeMillis();
                logger.info(String.format(Locale.ROOT, "Intersection of iso-cells in %d ms." +
                        " Begin insertion in database..", (int)(endOfIntersects - endCells)));
                PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO "+outputTable+" VALUES (?, ?, ?)");
                int batchSize = 0;
                for(IsoEntry isoEntry : deque) {
                    Geometry geom = isoEntry.cell;
                    if(geom instanceof Polygon) {
                        geom = geometyFactory.createMultiPolygon(new Polygon[]{(Polygon) geom});
                    }
                    geom.setSRID(srid);
                    insertStatement.setInt(1, isoEntry.index);
                    insertStatement.setObject(2, geom);
                    insertStatement.setString(3, isoEntry.isoLvl);
                    insertStatement.addBatch();
                    batchSize++;
                    if (batchSize >= BATCH_MAX_SIZE) {
                        insertStatement.executeBatch();
                        batchSize = 0;
                    }
                }
                if (batchSize > 0) {
                    insertStatement.executeBatch();
                }
                long endOfInsertion = System.currentTimeMillis();
                outputTables.add(outputTable);
                logger.info(String.format(Locale.ROOT,
                        "NoiseMap grid cell inserted into %s. Total operation time in %d ms" ,
                        outputTable,(int)(endOfInsertion - startMerge)));
            }

        }
        return outputTables;
    }

    /**
     * Merge geojson files with the same file name
     * prefix[NUMJOB]suffix[TABLENAME].csv
     * @param connection database connection
     * @param folder folder that contains geojson files
     * @param prefix common prefix before the number
     * @param suffix common suffix after the number
     * @return Tables created
     */
    public static List<String> mergeCSV(Connection connection,String folder, String prefix, String suffix, Set<String> tableFilter) throws SQLException {
        String extension = ".csv";
        File workingFolder = new File(folder);
        // Search files that match expected file name format
        String[] files = workingFolder.list(new CSVFileFilter(prefix, suffix));
        if(files == null) {
            return new ArrayList<>();
        }
        Map<String, ArrayList<String>> tableNameToFileNames = new HashMap<>();
        for(String fileName : files) {
            // Extract tableName from file name
            String tableName = fileName.substring(fileName.indexOf(suffix, prefix.length()) + suffix.length(),
                    fileName.length() - extension.length());
            if(tableFilter.isEmpty() || tableFilter.contains(tableName.toUpperCase(Locale.ROOT))) {
                ArrayList<String> fileNames;
                if (!tableNameToFileNames.containsKey(tableName)) {
                    fileNames = new ArrayList<>();
                    tableNameToFileNames.put(tableName, fileNames);
                } else {
                    fileNames = tableNameToFileNames.get(tableName);
                }
                fileNames.add(fileName);
            }
        }
        List<String> createdTables = new ArrayList<>();
        for(Map.Entry<String, ArrayList<String>> entry : tableNameToFileNames.entrySet()) {
            String finalTableName = entry.getKey().toUpperCase(Locale.ROOT);
            try(Statement st = connection.createStatement()) {
                ArrayList<String> fileNames = entry.getValue();
                if(fileNames.isEmpty()) {
                    continue;
                }
                // Copy the first table as a new table
                String firstFileName = fileNames.remove(0);
                st.execute("DROP TABLE IF EXISTS " + finalTableName);
                st.execute("CREATE TABLE "+finalTableName+" AS SELECT * FROM CSVREAD('" + new File(workingFolder, firstFileName).getAbsolutePath() + "');");
                createdTables.add(finalTableName);
                for(String fileName : fileNames) {
                    // insert into the existing table
                    st.execute("INSERT INTO " + finalTableName + " SELECT * FROM CSVREAD('" + new File(workingFolder, fileName).getAbsolutePath() + "');");
                }
            }
        }
        return createdTables;
    }

    /**
     * Merge geojson files with the same file name
     * prefix[NUMJOB]suffix[TABLENAME].geojson
     * @param connection database connection
     * @param folder folder that contains geojson files
     * @param prefix common prefix before the number
     * @param suffix common suffix after the number
     * @return Tables created
     */
    public static List<String> mergeGeoJSON(Connection connection,String folder, String prefix, String suffix) throws SQLException {
        String extension = ".geojson";
        File workingFolder = new File(folder);
        // Search files that match expected file name format
        String[] files = workingFolder.list(new GEOJSONFileFilter(prefix, suffix));
        if(files == null) {
            return new ArrayList<>();
        }
        Map<String, ArrayList<String>> tableNameToFileNames = new HashMap<>();
        for(String fileName : files) {
            // Extract tableName from file name
            String tableName = fileName.substring(fileName.indexOf(suffix, prefix.length()) + suffix.length(),
                    fileName.length() - extension.length());
            ArrayList<String> fileNames;
            if(!tableNameToFileNames.containsKey(tableName)) {
                fileNames = new ArrayList<>();
                tableNameToFileNames.put(tableName, fileNames);
            } else {
                fileNames = tableNameToFileNames.get(tableName);
            }
            fileNames.add(fileName);
        }
        List<String> createdTables = new ArrayList<>();
        for(Map.Entry<String, ArrayList<String>> entry : tableNameToFileNames.entrySet()) {
            String finalTableName = entry.getKey().toUpperCase(Locale.ROOT);
            try(Statement st = connection.createStatement()) {
                ArrayList<String> fileNames = entry.getValue();
                // Copy the first table as a new table
                while (!fileNames.isEmpty()) {
                    String fileName = fileNames.remove(0);
                    String tableName = fileName.substring(0, fileName.length() - extension.length()).toUpperCase(Locale.ROOT);
                    st.execute("DROP TABLE IF EXISTS " + tableName);
                    st.execute("CALL GEOJSONREAD('" + new File(workingFolder, fileName).getAbsolutePath() + "','" + tableName + "');");
                    List<String> columnNames = JDBCUtilities.getColumnNames(connection, tableName);
                    if(!columnNames.isEmpty()) {
                        // The file maybe empty, so ignore this file if there is no columns
                        st.execute("DROP TABLE IF EXISTS " + finalTableName);
                        st.execute("CREATE TABLE " + finalTableName + " AS SELECT * FROM " + tableName);
                        createdTables.add(finalTableName);
                        break;
                    }
                    st.execute("DROP TABLE IF EXISTS " + tableName);
                }
                for(String fileName : fileNames) {
                    // Link to shp file into the database
                    String tableName = fileName.substring(0, fileName.length() - extension.length()).toUpperCase(Locale.ROOT);
                    st.execute("DROP TABLE IF EXISTS " + tableName);
                    st.execute("CALL GEOJSONREAD('" + new File(workingFolder, fileName).getAbsolutePath() + "','" + tableName + "');");
                    // insert into the existing table
                    List<String> columnNames = JDBCUtilities.getColumnNames(connection, tableName);
                    if(!columnNames.isEmpty()) {
                        st.execute("INSERT INTO " + finalTableName + " SELECT * FROM " + tableName);
                    }
                    st.execute("DROP TABLE IF EXISTS " + tableName);
                }
            }
        }
        return createdTables;
    }


    /**
     * @param nmConnection
     * @param progressVisitor
     * @param numberOfJobs
     * @param workingDirectory
     * @return List of UUEID
     * @throws SQLException
     * @throws IOException
     */
    public static NoiseModellingInstance.ClusterConfiguration generateClusterConfig(Connection nmConnection, ProgressVisitor progressVisitor, int numberOfJobs, String workingDirectory) throws SQLException, IOException {
        NoiseModellingInstance.ClusterConfiguration clusterConfiguration = new NoiseModellingInstance.ClusterConfiguration();
        // Sum UUEID roads length
        // because the length of the roads should be proportional with the work load
        // Can't have more jobs than UUEID
        Sql sql = new Sql(nmConnection);
        int numberOfUUEID = asInteger(sql.firstRow("SELECT COUNT(DISTINCT UUEID) CPT FROM ROADS").get("CPT"));
        numberOfJobs = Math.min(numberOfUUEID, numberOfJobs);
        // Distribute UUEID over numberOfJobs (least road length receive the next one
        List<JobElement> jobElementList = new ArrayList<>(numberOfJobs);
        for(int i=0; i < numberOfJobs; i++) {
            jobElementList.add(new JobElement());
        }
        sql.execute("DROP TABLE IF EXISTS SRC_UUEIDS");
        sql.execute("CREATE TABLE SRC_UUEIDS(UUEID varchar, weight double, src_type int)");
        sql.execute("INSERT INTO SRC_UUEIDS SELECT UUEID , SUM(st_length(the_geom)) weight, 0 src_type  FROM ROADS r GROUP BY uueid");

        if(JDBCUtilities.tableExists(nmConnection, "LW_RAILWAY")) {
            sql.execute("INSERT INTO SRC_UUEIDS SELECT UUEID , SUM(st_length(the_geom)) weight, 1 src_type  FROM LW_RAILWAY r GROUP BY uueid");
        }
        try (ResultSet rs = nmConnection.createStatement().executeQuery("SELECT * FROM SRC_UUEIDS ORDER BY WEIGHT DESC")) {
            while (rs.next()) {
                String uueid = rs.getString("UUEID");
                if (rs.getInt("src_type") == 0) {
                    jobElementList.get(jobElementList.size() - 1).roadsUueid.add(uueid);
                    clusterConfiguration.roads_uueids.add(uueid);
                } else {
                    jobElementList.get(jobElementList.size() - 1).railsUueid.add(uueid);
                    clusterConfiguration.rails_uueids.add(uueid);
                }
                jobElementList.get(jobElementList.size() - 1).totalSourceLineLength += rs.getDouble("WEIGHT");
                // Sort by source line length
                Collections.sort(jobElementList);
            }
        }
        AtomicInteger nodeId = new AtomicInteger();
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootDoc = mapper.createArrayNode();
        // create lists so that each node have at least the quota length of roads (except the last one)
        for (JobElement jobElement : jobElementList) {
            ObjectNode nodeDoc = mapper.createObjectNode();
            rootDoc.add(nodeDoc);
            nodeDoc.put("nodeId", nodeId.getAndIncrement());
            nodeDoc.put("node_sum_length", jobElement.totalSourceLineLength);
            ArrayNode nodeUUEIDs = mapper.createArrayNode();
            nodeDoc.set("roads_uueids", nodeUUEIDs);
            for(String uueid : jobElement.roadsUueid) {
                nodeUUEIDs.add(uueid);
            }
            nodeUUEIDs = mapper.createArrayNode();
            nodeDoc.set("rails_uueids", nodeUUEIDs);
            for(String uueid : jobElement.railsUueid) {
                nodeUUEIDs.add(uueid);
            }
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(workingDirectory, "cluster_config.json"), rootDoc);
        return clusterConfiguration;
    }

    public Object LoadNoiselevel(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s42_Load_Noise_level.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public Object Isosurface(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s5_Isosurface.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }


    public Object Export(Connection nmConnection, ProgressVisitor progressVisitor) throws SQLException, IOException {
        GroovyShell shell = new GroovyShell();
        Script process= shell.parse(new File("../script_groovy", "s7_Export.groovy"));
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("confId", configuration.getConfigurationId());
        inputs.put("workingDirectory", configuration.getWorkingDirectory());
        inputs.put("progressVisitor", progressVisitor);
        inputs.put("databaseUser", configuration.getDataBaseConfig().user);
        inputs.put("databasePassword", configuration.getDataBaseConfig().password);
        inputs.put("batchSize", 1000);
        inputs.put("inputServer", "cloud");
        return process.invokeMethod("exec", new Object[] {nmConnection, inputs});
    }

    public static void pullFromSSH(ChannelSftp c, ProgressVisitor progressVisitor, String from, String to) throws IOException, SftpException {
        Vector v = c.ls(from);
        ProgressVisitor dirProg = progressVisitor.subProcess(v.size());
        File rootFolder = new File(to);
        if(!rootFolder.exists()) {
            logger.debug("mkdir " + to);
            if(!rootFolder.mkdir()) {
                return;
            }
        }
        for(Object e : v) {
            if(e instanceof ChannelSftp.LsEntry) {
                ChannelSftp.LsEntry sftpEntry = (ChannelSftp.LsEntry)e;
                File remoteFile = new File(from, sftpEntry.getFilename());
                File localFile = new File(to, sftpEntry.getFilename());
                if(sftpEntry.getAttrs().isDir()) {
                    if(!sftpEntry.getAttrs().isLink() && !sftpEntry.getFilename().startsWith(".")) {
                        pullFromSSH(c, dirProg, remoteFile.getAbsolutePath(), localFile.getAbsolutePath());
                    }
                } else {
                    logger.debug("get " + remoteFile.getAbsolutePath() + " " + localFile.getAbsolutePath());
                    c.get(remoteFile.getAbsolutePath(), localFile.getAbsolutePath());
                }
            }
            dirProg.endStep();
        }
    }

    private static class WalkFileVisitor implements FileVisitor<Path> {
        Set<Path> ignoredPaths = new HashSet<>();
        File parentFile;
        ChannelSftp c;
        String to;

        public WalkFileVisitor(Set<Path> ignoredPaths, File parentFile, ChannelSftp c, String to) {
            this.ignoredPaths = ignoredPaths;
            this.parentFile = parentFile;
            this.c = c;
            this.to = to;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
            if(ignoredPaths.contains(path)) {
                return FileVisitResult.SKIP_SUBTREE;
            } else {
                // do create directory
                Path destFolder = parentFile.toPath().relativize(path);
                File remoteFolder;
                if(to.isEmpty()) {
                    remoteFolder = new File(destFolder.toString());
                } else {
                    remoteFolder = new File(to, destFolder.toString());
                }
                if(!remoteFolder.toString().isEmpty()) {
                    logger.debug("mkdir " + remoteFolder);
                    if (c != null) {
                        try {
                            c.mkdir(remoteFolder.toString());
                        } catch (SftpException ex) {
                            // dir exist / ignore
                            logger.debug(ex.getLocalizedMessage());
                        }
                    }
                }
                return  FileVisitResult.CONTINUE;
            }
        }

        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
            if(!ignoredPaths.contains(path)) {
                // transfer file
                Path dest = parentFile.toPath().relativize(path);
                File remoteFile;
                if (to.isEmpty()) {
                    remoteFile = new File(dest.toString());
                } else {
                    remoteFile = new File(to, dest.toString());
                }
                if (c != null) {
                    logger.debug("put " + path.toFile() + " " + remoteFile);
                    try {
                        c.put(path.toFile().toString(), remoteFile.toString());
                    } catch (SftpException ex) {
                        throw new IOException(ex);
                    }
                }
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }


    public static void pushToSSH(ChannelSftp c, ProgressVisitor progressVisitor, String from, String to, boolean createSubDirectory, Set<Path> ignorePaths) throws IOException, SftpException, InterruptedException {
        // List All files
        logger.info("Push files from " + from + " to " + to + " through SSH");
        File fromFile = new File(from);
        File parentFile = fromFile;
        if (createSubDirectory) {
            parentFile = fromFile.getParentFile();
        }
        int retries = 0;
        while (true) {
            try {
                WalkFileVisitor walkFileVisitor = new WalkFileVisitor(ignorePaths, parentFile, c, to);
                Files.walkFileTree(fromFile.toPath(), walkFileVisitor);
                break;
            } catch (IOException ex) {
                retries++;
                logger.warn("Could not send files, will retry again " + (MAX_SSH_SEND_RETRY - retries) + " times");
                if(progressVisitor.isCanceled()) {
                    throw ex;
                }
                Thread.sleep(POLL_SLURM_STATUS_TIME);
                if (retries > MAX_SSH_SEND_RETRY) {
                    throw ex;
                }

            }
        }
    }

    private static final String[] names = {
            "ssh-dss",
            "ssh-rsa",
            "ecdsa-sha2-nistp256",
            "ecdsa-sha2-nistp384",
            "ecdsa-sha2-nistp521"
    };

    protected static int name2type(String name){
        for(int i = 0; i < names.length; i++){
            if(names[i].equals(name)){
                return i + 1;
            }
        }
        return 6;
    }

    public void recurseMkDir(ChannelSftp c, String folder) throws SftpException {
        Path folderPath = new File(folder).toPath();
        int folderCount = 1;
        while(folderCount <= folderPath.getNameCount()) {
            Path firstFolder = folderPath.subpath(0, folderCount);
            Path parentPath;
            if(folderCount > 1) {
                parentPath = folderPath.subpath(0, folderCount - 1);
            } else {
                parentPath = new File(c.getHome()).toPath();
            }
            boolean foundFolder = false;
            try {
                Vector files = c.ls(parentPath.toString());
                for (Object oEntry : files) {
                    if (oEntry instanceof ChannelSftp.LsEntry) {
                        if (((ChannelSftp.LsEntry) oEntry).getFilename().equals(firstFolder.getFileName().toString())) {
                            foundFolder = true;
                            break;
                        }
                    }
                }
            } catch (SftpException ex) {
                logger.info("Error while checking folder \"" + parentPath + "\" content");
                throw ex;
            }
            if(!foundFolder) {
                try {
                    c.mkdir(firstFolder.toString());
                } catch (SftpException ex) {
                    logger.info("Error while trying to create folder \"" + firstFolder + "\"");
                    throw ex;
                }
            }
            folderCount++;
        }
    }

    public static Session openSshSession(SlurmConfig slurmConfig) throws JSchException {
        JSch jsch=new JSch();
        HostKeyRepository hkr = jsch.getHostKeyRepository();
        // Add host identification if not already in the repository
        if(hkr.getHostKey(slurmConfig.host, slurmConfig.serverKeyType).length == 0) {
            hkr.add(new HostKey(slurmConfig.host, name2type(slurmConfig.serverKeyType),
                    Base64.getDecoder().decode(slurmConfig.serverKey)), null);
        }

        jsch.addIdentity(slurmConfig.sshFile, slurmConfig.sshFilePassword);
        // Load known_hosts
        Session session = jsch.getSession(slurmConfig.user, slurmConfig.host, slurmConfig.port);
        try {
            session.connect(SFTP_TIMEOUT);
            logger.info("Successfully connected to the server " + slurmConfig.host);
        } catch (JSchException ex) {
            if(ex.getMessage().contains("UnknownHostKey") || ex.getMessage().contains("has been changed")) {
                // Connect but only print the expected key
                Session insecureSession = jsch.getSession(slurmConfig.user, slurmConfig.host, slurmConfig.port);
                insecureSession.setConfig("StrictHostKeyChecking", "no");
                insecureSession.connect(SFTP_TIMEOUT);
                HostKey hk=insecureSession.getHostKey();
                logger.error("Unknown host. Use the following configuration in config.yaml file if you trust this server:\n" +
                        " serverKeyType:\"" + hk.getType() + "\"\n serverKey:\""+hk.getKey()+"\"");
                insecureSession.disconnect();
            }
            throw ex;
        }
        return session;
    }

    public List<String> runCommand(Session session, String command) throws JSchException, IOException {
        return runCommand(session, command, true);
    }
    public List<String> runCommand(Session session, String command, boolean logResult) throws JSchException, IOException {
        return runCommand(session, command, logResult, new AtomicLong());
    }
    public List<String> runCommand(Session session, String command, boolean logResult, AtomicLong readBytes) throws JSchException, IOException {
        List<String> lines = new ArrayList<>();
        ChannelExec shell = (ChannelExec) session.openChannel("exec");
        try {
            shell.setCommand(command);
            shell.setInputStream(null);
            shell.setErrStream(System.err);

            InputStream in = shell.getInputStream();

            CountingInputStream countingInputStream = new CountingInputStream(in);
            InputStreamReader inputStreamReader = new InputStreamReader(countingInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            // run command
            shell.connect(SFTP_TIMEOUT);

            while (true) {
                String line = bufferedReader.readLine();
                if (line != null) {
                    if(logResult) {
                        logger.info(line);
                    }
                    lines.add(line);
                } else {
                    if (shell.isClosed()) {
                        if(shell.getExitStatus() != 0) {
                            logger.error(String.format("Command %s \n exit-status: %d", command, shell.getExitStatus()));
                        }
                    } else {
                        logger.warn("Stream is closed but the channel is still open");
                    }
                    break;
                }
            }
            readBytes.addAndGet(countingInputStream.getBytesRead());
        } finally {
            shell.disconnect();
        }

        return lines;
    }


    public static List<SlurmJobStatus> parseSlurmStatus(List<String> commandOutput, int jobId) {
        List<SlurmJobStatus> slurmJobStatus = new ArrayList<>();
        try {
            for(String line : commandOutput) {
                line = line.trim();
                if (line.contains(BATCH_FILE_NAME)) {
                    List<String> columns = Collections.list(new StringTokenizer(line, " ")).stream().map(token -> (String) token).collect(Collectors.toList());
                    String jobColumn = columns.get(0);
                    int jobColumnId = Integer.parseInt(jobColumn.substring(0, jobColumn.lastIndexOf("_")));
                    String jobIndexString = jobColumn.substring(jobColumn.lastIndexOf("_") + 1);
                    // jobIndexString may contain this [0-31]
                    if (jobColumnId == jobId && !jobIndexString.contains("-")) {
                        int jobIndex = Integer.parseInt(jobIndexString);
                        String status = columns.get(2);
                        slurmJobStatus.add(new SlurmJobStatus(status, jobIndex));
                    }
                }
            }
        } catch (NumberFormatException | IndexOutOfBoundsException ex) {
            // ignore
            logger.error("Error while parsing job status", ex);
        }
        return slurmJobStatus;
    }

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
    void logSlurmJobs(Session session, Map<String, Long> bytesReadInFiles) {
        try {
            List<String>  output = runCommand(session, String.format("cd %s && find ./*.out -type f -printf \"%%s,%%f\\n\"", configuration.remoteJobFolder), false);
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
                    runCommand(session, String.format("cd %s && tail -c +%d %s", configuration.remoteJobFolder, alreadyReadBytes, file.fileName), true, readBytes);
                    bytesReadInFiles.put(file.fileName, alreadyReadBytes + readBytes.get());
                }
            }
        } catch (JSchException | IOException e) {
            logger.error("Error while reading remote log files", e);
        }
    }

    private boolean isSlurmJobsFinished(Session session, ProgressVisitor slurmJobProgress) throws JSchException, IOException, CancellationException {
        List<String> output = runCommand(session, String.format("sacct --format JobID%%18,Jobname%%30,State,Elapsed,TotalCPU -j %d", configuration.slurmJobId));
        List<SlurmJobStatus> jobStatusList = parseSlurmStatus(output, configuration.slurmJobId);
        int finishedStatusCount = 0;
        for(SlurmJobStatus s : jobStatusList) {
            if(slurmStateMap.containsKey(s.status) && slurmStateMap.get(s.status).finished) {
                finishedStatusCount++;
            }
            if(slurmStateMap.containsKey(s.status) && slurmStateMap.get(s.status).error) {
                // If one of the process fail, cancel the computation and set the computation as failed
                runCommand(session, String.format("scancel %d", configuration.slurmJobId));
                throw new CancellationException("Slurm job has failed");
            }
        }
        // increase progress if needed
        if(oldFinishedJobs != finishedStatusCount) {
            for(int i=0; i < (finishedStatusCount - oldFinishedJobs); i++) {
                slurmJobProgress.endStep();
            }
            oldFinishedJobs = finishedStatusCount;
        }
        return finishedStatusCount == configuration.slurmConfig.maxJobs;
    }

    public void slurmInitAndStart(SlurmConfig slurmConfig, ProgressVisitor progressVisitor) throws JSchException, IOException, SftpException, InterruptedException, SQLException {
        int connectionRetry = MAX_CONNECTION_RETRY;
        Session session = openSshSession(slurmConfig);
        try {
            Channel sftp;
            sftp = session.openChannel("sftp");
            try {
                sftp.connect(SFTP_TIMEOUT);
                ChannelSftp c = (ChannelSftp) sftp;
                recurseMkDir(c, configuration.remoteJobFolder);
                // copy computation core
                File computationCoreFolder = new File(new File("").getAbsoluteFile().getParentFile(), "computation_core");
                logger.debug("Computation core folder: " + computationCoreFolder);
                String libFolder = new File(computationCoreFolder, "build" + File.separator + "install"+ File.separator + "computation_core" + File.separator + "lib").toString();
                pushToSSH(c, progressVisitor, libFolder, configuration.remoteJobFolder, true, new HashSet<>());
                // copy data
                pushToSSH(c, progressVisitor, configuration.workingDirectory, configuration.remoteJobFolder,
                        false, Collections.singleton(new File(configuration.workingDirectory,
                                POST_PROCESS_RESULT_DIRECTORY_NAME).toPath()));
                // copy slurm file
                c.put(new File(computationCoreFolder, BATCH_FILE_NAME).toString(), new File(configuration.remoteJobFolder, BATCH_FILE_NAME).toString());
            } finally {
                sftp.disconnect();
            }
            logger.info("File transferred running computation core on cluster");
            // Run batch jobs
            List<String> output = runCommand(session,
                    String.format("cd %s && sbatch --array=0-%d %s", configuration.remoteJobFolder,
                            configuration.slurmConfig.maxJobs - 1, BATCH_FILE_NAME));
            // Parse Cluster Job identifier
            if(output.isEmpty()) {
                logger.info("Cannot read the job identifier");
                throw new IOException("No output in ssh command");
            }
            for(String line : output) {
                line = line.trim();
                if(line.startsWith("Submitted batch job")) {
                    configuration.slurmJobId = Integer.parseInt(line.substring(line.lastIndexOf(" ")).trim());
                    break;
                }
            }
            if(configuration.slurmJobId == -1) {
                logger.info("Cannot read the job identifier");
                throw new IOException("Not expected output in ssh command");
            }
            try (Connection connection = plamadeDataSource.getConnection()) {
                PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET SLURM_JOB_ID = ? WHERE PK_JOB = ?");
                st.setInt(1, configuration.slurmJobId);
                st.setInt(2, configuration.getTaskPrimaryKey());
                st.execute();
            } catch (SQLException | SecurityException ex) {
                logger.error(ex.getLocalizedMessage(), ex);
            }
            ProgressVisitor slurmJobProgress = progressVisitor.subProcess(configuration.slurmConfig.maxJobs);
            Map<String, Long> bytesReadInFiles = new HashMap<>();
            while(true) {
                if(progressVisitor.isCanceled()) {
                    runCommand(session, String.format("scancel %d", configuration.slurmJobId));
                    break;
                }
                long lastPullTime = System.currentTimeMillis();
                logSlurmJobs(session, bytesReadInFiles);
                // Check status of jobs on cluster side
                try {
                    if (isSlurmJobsFinished(session, slurmJobProgress)) {
                        break;
                    }
                } catch (JSchException ex) {
                    // retry open a connection
                    while (true) {
                        try {
                            logger.error("Error while checking jobs, retry " +
                                    (MAX_CONNECTION_RETRY - connectionRetry)+ "/" + MAX_CONNECTION_RETRY, ex);
                            session = openSshSession(slurmConfig);
                            connectionRetry = MAX_CONNECTION_RETRY;
                            break;
                        } catch (JSchException ex2) {
                            if (connectionRetry-- <= 0) {
                                throw new IOException("Error while checking jobs", ex2);
                            }
                            Thread.sleep(POLL_SLURM_STATUS_TIME);
                        }
                    }
                }
                // Sleep up to POLL_SLURM_STATUS_TIME
                Thread.sleep(Math.max(1000, POLL_SLURM_STATUS_TIME - (System.currentTimeMillis() - lastPullTime)));
            }
            // retrieve data
            sftp = session.openChannel("sftp");
            String resultDir;
            try {
                sftp.connect(SFTP_TIMEOUT);
                ChannelSftp c = (ChannelSftp) sftp;
                String remoteHome = c.getHome();
                resultDir = new File(remoteHome, String.format("results_%d", configuration.slurmJobId)).getAbsolutePath();
                pullFromSSH(c, progressVisitor,
                        resultDir,
                        new File(configuration.workingDirectory, RESULT_DIRECTORY_NAME).getAbsolutePath());
            } finally {
                sftp.disconnect();
            }
            // Remote cleaning
            logger.info("Clean remote data");
            runCommand(session, String.format("rm -rvf %s", configuration.remoteJobFolder));
            runCommand(session, String.format("rm -rvf %s", resultDir));
            // merge files and load it in database
            try(Connection nmConnection = nmDataSource.getConnection()){
                mergeCSV(nmConnection, new File(configuration.workingDirectory, RESULT_DIRECTORY_NAME).getAbsolutePath(),
                        "out_", "_", Collections.singleton("POPULATION_EXPOSURE"));
                Statement st = nmConnection.createStatement();
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDPEOPLE INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDAREA REAL;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDDWELLINGS INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDHOSPITALS INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN EXPOSEDSCHOOLS INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN CPI INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN HA INT;");
                st.execute("ALTER TABLE POPULATION_EXPOSURE ALTER COLUMN HSD INT;");
            }
        } finally {
            session.disconnect();
        }

    }

    void setJobState(JOB_STATES newState) {
        try (Connection connection = plamadeDataSource.getConnection()) {
            PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET STATE = ? WHERE PK_JOB = ?");
            st.setString(1, newState.toString());
            st.setInt(2, configuration.getTaskPrimaryKey());
            st.execute();
        } catch (SQLException | SecurityException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
    }

    /**
     * Export provided spatial table names to a specified folder
     * @param connection JDBC connection
     * @param tablesToExport Table name to export
     * @param folder Folder to save shape files
     * @throws SQLException
     * @throws IOException
     */
    public static void exportTables(Connection connection, List<String> tablesToExport, String folder, int srid) throws SQLException, IOException {
        for(String tableName : tablesToExport) {
            if(JDBCUtilities.tableExists(connection, tableName)) {
                if(srid != 0) {
                    String geometryColumnName = GeometryTableUtilities.getGeometryColumnNames(connection, tableName).get(0);
                    List<String> columnNames = JDBCUtilities.getColumnNames(connection, tableName);
                    columnNames.remove(geometryColumnName);
                    try(ResultSet resultSet = connection.createStatement().executeQuery(
                            "SELECT COUNT(*) FROM " + tableName + " WHERE " + geometryColumnName + " IS NOT NULL")) {
                        resultSet.next();
                        int rowCount = resultSet.getInt(1);
                        if(rowCount < 1) {
                            continue;
                        }
                    }
                    StringBuilder sb = new StringBuilder("(SELECT ST_TRANSFORM(");
                    sb.append(geometryColumnName);
                    sb.append(", ");
                    sb.append(srid);
                    sb.append(") ");
                    sb.append(geometryColumnName);
                    for (String columnName : columnNames) {
                        sb.append(", ");
                        sb.append(columnName);
                    }
                    sb.append(" FROM ");
                    sb.append(tableName);
                    sb.append(" WHERE ");
                    sb.append(geometryColumnName);
                    sb.append(" IS NOT NULL)");
                    GeoJsonWrite.exportTable(connection, new File(folder, tableName + ".geojson").getAbsolutePath(), sb.toString(), true);
                } else {
                    GeoJsonWrite.exportTable(connection, new File(folder, tableName + ".geojson").getAbsolutePath(), tableName, true);
                }
            }
        }
    }

    public void startNoiseModelling(ProgressVisitor progressVisitor, NoiseModellingInstance.ClusterConfiguration clusterConfiguration) throws SQLException, IOException {
        Main.printBuildIdentifiers(logger);
        String workingDir = new File(configuration.workingDirectory, RESULT_DIRECTORY_NAME).getAbsolutePath();
        try (Connection nmConnection = new ConnectionWrapper(nmDataSource.getConnection())) {
            NoiseModellingInstance.startNoiseModelling(nmConnection, progressVisitor, clusterConfiguration, workingDir, 0);
        }
    }

    public void exportCSV(Connection connection, String path, String tableName) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("CALL CSVWRITE(?, ?)");
        ps.setString(1, path);
        ps.setString(2, tableName);
        ps.execute();
    }

    @Override
    public void run() {
        Thread.currentThread().setName("JOB_" + configuration.getTaskPrimaryKey());
        isRunning = true;
        setJobState(JOB_STATES.RUNNING);
        try (Connection connection = plamadeDataSource.getConnection()) {
            PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET BEGIN_DATE = ? WHERE PK_JOB = ?");
            Timestamp t = new Timestamp(System.currentTimeMillis());
            st.setObject(1, t);
            st.setInt(2, configuration.getTaskPrimaryKey());
            st.execute();
        } catch (SQLException | SecurityException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
        try {
            // create folder
            File workingDir = new File(configuration.workingDirectory);
            if (workingDir.exists() && workingDir.isDirectory()) {
                if (workingDir.getAbsolutePath().startsWith(new File("").getAbsolutePath())) {
                    if (!workingDir.delete()) {
                        logger.error("Cannot delete the working directory\n" + configuration.workingDirectory);
                        return;
                    }
                } else {
                    logger.error(String.format(Locale.ROOT, "Can delete only sub-folder \n%s\n%s", new File("").getAbsolutePath(), workingDir.getAbsolutePath()));
                }
            }
            if(!workingDir.exists()) {
                if (!(workingDir.mkdirs())) {
                    logger.error("Cannot create the working directory\n" + configuration.workingDirectory);
                    return;
                }
            }
            nmDataSource = createDataSource("", "", configuration.workingDirectory, H2GIS_DATABASE_NAME, false);

            // Download data from external database
            ProgressVisitor progressVisitor = configuration.progressVisitor;
            ProgressVisitor subProg = progressVisitor.subProcess(8);
            File outDir = new File(configuration.workingDirectory, POST_PROCESS_RESULT_DIRECTORY_NAME);
            if (!outDir.exists()) {
                if (!outDir.mkdir()) {
                    return;
                }
            }
            NoiseModellingInstance.ClusterConfiguration clusterConfiguration;
            try (Connection nmConnection = nmDataSource.getConnection()) {
                importData(nmConnection, subProg);
                exportTables(nmConnection,
                        Arrays.asList("ROADS", "BUILDINGS_SCREENS", "LANDCOVER", "SCREENS", "RAIL_SECTIONS"),
                        outDir.getAbsolutePath(), 4326);

                if (subProg.isCanceled()) {
                    setJobState(JOB_STATES.CANCELED);
                    return;
                }
                makeGrid(nmConnection, configuration.getConfigurationId());
                subProg.endStep();
                makeEmission(nmConnection);
                exportTables(nmConnection, Arrays.asList("LW_ROADS", "LW_RAILWAY"), outDir.getAbsolutePath(), 4326);
                subProg.endStep();
                int maxJobs = 1;
                if(configuration.computeOnCluster) {
                    maxJobs = configuration.slurmConfig.maxJobs;
                }
                clusterConfiguration = generateClusterConfig(nmConnection, subProg, maxJobs, configuration.workingDirectory);
                subProg.endStep();
            }
            if(configuration.computeOnCluster) {
                // Compact database
                try (Connection nmConnection = nmDataSource.getConnection()) {
                    nmConnection.createStatement().execute("SHUTDOWN COMPACT");
                } catch (JdbcSQLNonTransientConnectionException ex) {
                    // ignore
                }
                slurmInitAndStart(configuration.slurmConfig, subProg);
            } else {
                startNoiseModelling(subProg, clusterConfiguration);
            }
            try (Connection nmConnection = nmDataSource.getConnection()) {
                exportCSV(nmConnection,
                        new File(outDir.getAbsolutePath(), "POPULATION_EXPOSURE.csv").getAbsolutePath(), "POPULATION_EXPOSURE");
                String resultDirectoryFullPath = new File(configuration.workingDirectory, RESULT_DIRECTORY_NAME).getAbsolutePath();
                List<String> createdTables = mergeGeoJSON(nmConnection,
                        resultDirectoryFullPath,
                        "out_", "_");
                createdTables.addAll(mergeCBS(nmConnection, CBS_GRID_SIZE, CBS_MAIN_GRID_SIZE,
                        subProg));
                // Save merged final tables
                exportTables(nmConnection, createdTables, outDir.getAbsolutePath(), 4326);
                subProg.endStep();
                logger.info(Export(nmConnection, subProg).toString());
                subProg.endStep();
            }
            setJobState(JOB_STATES.COMPLETED);
        } catch (SQLException ex) {
            while(ex != null) {
                logger.error(ex.getLocalizedMessage(), ex);
                ex = ex.getNextException();
            }
            setJobState(JOB_STATES.FAILED);
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            setJobState(JOB_STATES.FAILED);
        } finally {
            // Update Job data
            try (Connection connection = plamadeDataSource.getConnection()) {
                PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET END_DATE = ?, PROGRESSION = 100 WHERE PK_JOB = ?");
                st.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                st.setInt(2, configuration.getTaskPrimaryKey());
                st.execute();
            } catch (SQLException | SecurityException ex) {
                logger.error(ex.getLocalizedMessage(), ex);
            }
            isRunning = false;
            // Save logs
            try {
                List<String> rows = getAllLines(String.valueOf(configuration.taskPrimaryKey), -1);
                try(FileWriter writer = new FileWriter(new File(configuration.workingDirectory, JOB_LOG_FILENAME))) {
                    for(String row : rows) {
                        writer.write(row + "\n");
                    }
                }
            }catch (IOException ex) {
                logger.error("Error while exporting logs");
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        configuration.progressVisitor.cancel();
        return false;
    }

    @Override
    public boolean isCancelled() {
        return configuration.progressVisitor.isCanceled();
    }

    @Override
    public boolean isDone() {
        return !isRunning;
    }

    @Override
    public String get() throws InterruptedException, ExecutionException {
        return "";
    }

    @Override
    public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return "";
    }

    public static class Configuration {
        private String workingDirectory;
        private int configurationId;
        private String inseeDepartment;
        private int taskPrimaryKey;
        private DataBaseConfig dataBaseConfig;
        private ProgressVisitor progressVisitor = new EmptyProgressVisitor();
        private SlurmConfig slurmConfig;
        private String remoteJobFolder;
        private int slurmJobId = -1;
        private int userPK;
        private boolean computeOnCluster = true;

        public Configuration(int userPk, String workingDirectory, int configurationId, String inseeDepartment, int taskPrimaryKey
                , DataBaseConfig dataBaseConfig, ProgressVisitor progressVisitor, String remoteJobFolder) {
            this.workingDirectory = workingDirectory;
            this.configurationId = configurationId;
            this.inseeDepartment = inseeDepartment;
            this.taskPrimaryKey = taskPrimaryKey;
            this.dataBaseConfig = dataBaseConfig;
            this.progressVisitor = progressVisitor;
            this.remoteJobFolder = remoteJobFolder;
            this.userPK = userPk;
        }

        public Configuration setComputeOnCluster(boolean computeOnCluster) {
            this.computeOnCluster = computeOnCluster;
            return this;
        }

        public int getSlurmJobId() {
            return slurmJobId;
        }

        public void setSlurmJobId(int slurmJobId) {
            this.slurmJobId = slurmJobId;
        }

        public int getUserPK() {
            return userPK;
        }

        public SlurmConfig getSlurmConfig() {
            return slurmConfig;
        }

        public void setSlurmConfig(SlurmConfig slurmConfig) {
            this.slurmConfig = slurmConfig;
        }

        public String getWorkingDirectory() {
            return workingDirectory;
        }

        public int getConfigurationId() {
            return configurationId;
        }

        public String getInseeDepartment() {
            return inseeDepartment;
        }

        public int getTaskPrimaryKey() {
            return taskPrimaryKey;
        }

        public DataBaseConfig getDataBaseConfig() {
            return dataBaseConfig;
        }
    }


    public static class SlurmJobStatus {
        public final String status;
        public final int taskId;

        public SlurmJobStatus(String status, int taskId) {
            this.status = status;
            this.taskId = taskId;
        }
    }

    public static class SlurmJobKnownStatus {
        public final String status;
        public final boolean finished;
        public final boolean error;

        public SlurmJobKnownStatus(String status, boolean finished, boolean error) {
            this.status = status;
            this.finished = finished;
            this.error = error;
        }
    }

    public static class FileAttributes {
        public final String fileName;
        public final long fileSize;

        public FileAttributes(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }
    }
}
