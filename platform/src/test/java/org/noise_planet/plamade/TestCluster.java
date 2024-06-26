package org.noise_planet.plamade;

import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.noise_planet.nmcluster.NoiseModellingInstance;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.noise_planet.plamade.process.NoiseModellingProfileReport;
import org.noise_planet.plamade.process.NoiseModellingRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static junit.framework.TestCase.assertEquals;
import static org.noise_planet.nmcluster.NoiseModellingInstance.CBS_GRID_SIZE;

public class TestCluster {

    public static List<String> splitCommand(String command) {
        List<String> commandLines = new ArrayList<>();
        StringTokenizer s = new StringTokenizer(command, "\n");
        while(s.hasMoreTokens()) {
            commandLines.add(s.nextToken());
        }
        return commandLines;
    }

    @BeforeClass
    public static void initLogger() {
        BasicConfigurator.configure();
    }

    /**
     * Parse command output of
     * find ./*.out -type f -printf "%s,%f\n"
     */
    @Test
    public void testParseSelect() {
        String command = "7468,slurm-14927317_0.out\n" +
                "2694,slurm-14927317_1.out\n" +
                "8094,slurm-14927317_10.out\n" +
                "2417,slurm-14927317_11.out\n" +
                "6317,slurm-14927317_12.out\n" +
                "5875,slurm-14927317_13.out\n" +
                "7993,slurm-14927317_14.out\n" +
                "2945,slurm-14927317_15.out\n" +
                "2572,slurm-14927317_16.out\n" +
                "3345,slurm-14927317_17.out\n" +
                "2807,slurm-14927317_18.out\n" +
                "3275,slurm-14927317_19.out\n" +
                "8045,slurm-14927317_2.out\n" +
                "4444,slurm-14927317_20.out\n" +
                "7467,slurm-14927317_21.out\n" +
                "3420,slurm-14927317_22.out\n" +
                "2662,slurm-14927317_23.out\n" +
                "2471,slurm-14927317_24.out\n" +
                "2596,slurm-14927317_25.out\n" +
                "3143,slurm-14927317_26.out\n" +
                "5188,slurm-14927317_27.out\n" +
                "4507,slurm-14927317_28.out\n" +
                "6415,slurm-14927317_29.out\n" +
                "4939,slurm-14927317_3.out\n" +
                "13617,slurm-14927317_30.out\n" +
                "3673,slurm-14927317_31.out\n" +
                "8816,slurm-14927317_4.out\n" +
                "15190,slurm-14927317_5.out\n" +
                "6747,slurm-14927317_6.out\n" +
                "9322,slurm-14927317_7.out\n" +
                "2416,slurm-14927317_8.out\n" +
                "10503,slurm-14927317_9.out";
        List<String> commandLines = splitCommand(command);
        List<NoiseModellingRunner.FileAttributes> files = NoiseModellingRunner.parseLSCommand(commandLines);
        assertEquals(32, files.size());
        assertEquals("slurm-14927317_0.out", files.get(0).fileName);
        assertEquals(7468, files.get(0).fileSize);
        assertEquals("slurm-14927317_1.out", files.get(1).fileName);
        assertEquals(2694, files.get(1).fileSize);
        assertEquals("slurm-14927317_10.out", files.get(2).fileName);
        assertEquals(8094, files.get(2).fileSize);
        assertEquals("slurm-14927317_11.out", files.get(3).fileName);
        assertEquals(2417, files.get(3).fileSize);
    }

    @Test
    public void testParseSacct() {
        String command = "             JobID                        JobName      State \n" +
                "------------------ ------------------------------ ---------- \n" +
                "       14945373_31        noisemodelling_batch.sh  COMPLETED \n" +
                " 14945373_31.batch                          batch  COMPLETED \n" +
                "        14945373_0        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_0.batch                          batch  COMPLETED \n" +
                "        14945373_1        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_1.batch                          batch  COMPLETED \n" +
                "        14945373_2        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_2.batch                          batch  COMPLETED \n" +
                "        14945373_3        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_3.batch                          batch  COMPLETED \n" +
                "        14945373_4        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_4.batch                          batch  COMPLETED \n" +
                "        14945373_5        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_5.batch                          batch  COMPLETED \n" +
                "        14945373_6        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_6.batch                          batch  COMPLETED \n" +
                "        14945373_7        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_7.batch                          batch  COMPLETED \n" +
                "        14945373_8        noisemodelling_batch.sh  COMPLETED \n" +
                "  14945373_8.batch                          batch  COMPLETED \n";

        List<String> commandLines = splitCommand(command);
        List<NoiseModellingRunner.SlurmJobStatus> jobList = NoiseModellingRunner.parseSlurmStatus(commandLines, 14945373);
        assertEquals(10, jobList.size());
        assertEquals("COMPLETED", jobList.get(0).status);
        assertEquals(31, jobList.get(0).taskId);
        assertEquals("COMPLETED", jobList.get(1).status);
        assertEquals(0, jobList.get(1).taskId);
        assertEquals("COMPLETED", jobList.get(2).status);
        assertEquals(1, jobList.get(2).taskId);
    }

    @Test
    public void testParseSacctRegression1() {
        String command = "             JobID                        JobName      State \n" +
                "------------------ ------------------------------ ---------- \n" +
                "14947161_[0-31] noisemodelling_batch.sh PENDING 00:00:00 00:00:00\n";

        List<String> commandLines = splitCommand(command);
        List<NoiseModellingRunner.SlurmJobStatus> jobList = NoiseModellingRunner.parseSlurmStatus(commandLines, 14947161);
        assertEquals(0, jobList.size());
    }

    @Test
    public void testDebugNoiseProfile() throws Throwable {
        if(new File("/home/nicolas").exists()) {
            NoiseModellingProfileReport noiseModellingProfileReport = new NoiseModellingProfileReport();
            noiseModellingProfileReport.testDebugNoiseProfile("/home/nicolas/data/plamade/dep37",
                    new Coordinate(1.01821,47.44004, 4.0), "RL_FR_00_101",
                    NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL, 100, new File("/home/nicolas/data/plamade/dep37", "report.html"));
        }
    }

//    @Test
//    public void makeGridTest() throws SQLException, IOException, LayerDelaunayError {
//        DataSource ds = NoiseModellingInstance.createDataSource("", "", "/home/nicolas/data/plamade/dep69", "h2gisdb", false);
//        try (Connection sql = ds.getConnection()) {
//            Connection connection = new ConnectionWrapper(sql);
//            NoiseModellingInstance.exportTables(connection, Arrays.asList("BUILDINGS_SCREENS"), "target/", 4326);
//        }
//    }

//    @Test
//    public void makeGridTest() throws SQLException, IOException, LayerDelaunayError {
//        DataSource ds = NoiseModellingInstance.createDataSource("", "",
//                "/home/nicolas/data/plamade/dep44/", "h2gisdb", false);
//        try(Connection sql = ds.getConnection()) {
//            Connection connection = new ConnectionWrapper(sql);
//            //NoiseModellingInstance.makeGrid(sql, 4);
//            connection.createStatement().execute("DROP TABLE RECEIVERS IF EXISTS");
//            TriangleNoiseMap noiseMap = new TriangleNoiseMap("BUILDINGS_SCREENS", "");
//
//            // Avoid loading to much geometries when doing Delaunay triangulation
//            noiseMap.setMaximumPropagationDistance(800);
//            // Receiver height relative to the ground
//            noiseMap.setReceiverHeight(1.6);
//            // No receivers closer than road width distance
//            double roadWidth = 2.0;
//
//            double maxArea = 0;
//            noiseMap.setRoadWidth(roadWidth);
//            // No triangles larger than provided area
//            noiseMap.setMaximumArea(maxArea);
//            // Do not remove isosurface behind buildings
//            noiseMap.setIsoSurfaceInBuildings(true);
//            // Densification of receivers near sound sources
//            //modifNico noiseMap.setSourceDensification(sourceDensification)
//
//            noiseMap.initialize(connection, new EmptyProgressVisitor());
//            noiseMap.setExceptionDumpFolder("data_dir/");
//            AtomicInteger pk = new AtomicInteger(0);
//            noiseMap.setGridDim(25);
//            String triangleTable = "TRIANGLES_DELAUNAY";
//            String dumpPath = new File("").getAbsolutePath();
//            System.out.println("Dump to " + dumpPath);
//            noiseMap.setExceptionDumpFolder(dumpPath);
//            int i = 7;
//            int j = 15;
//            noiseMap.generateReceivers(connection, i, j, "RECEIVERS", triangleTable, pk);
//        }
//    }
//    @Test
//    public void exportTest() throws SQLException, IOException {
//        String workingDir = "/home/nicolas/data/plamade/dep85/";
//        DataSource ds = NoiseModellingInstance.createDataSource("", "",
//                workingDir, "h2gisdb", false);
//        try(Connection connection = ds.getConnection()) {
//            List<String> outputTables = Arrays.asList("ROADS", "BUILDINGS_SCREENS", "LANDCOVER", "SCREENS", "RAIL_SECTIONS");
//            NoiseModellingInstance.exportTables(connection, outputTables, workingDir, 4326);
//        }
//    }
//    @Test
//    public void mergeCBSTest() throws SQLException, IOException {
//        String workingDir = "/home/nicolas/data/plamade/dep05_1646821826645";
//        DataSource ds = NoiseModellingRunner.createDataSource("", "",
//                workingDir, "h2gisdb", false);
//        try(Connection sql = ds.getConnection()) {
//            List<String> outputTables = NoiseModellingRunner.mergeCBS(sql, NoiseModellingRunner.CBS_GRID_SIZE,
//                    NoiseModellingRunner.CBS_MAIN_GRID_SIZE,
//                    new RootProgressVisitor(1, true, 2));
//            for(String outputTable : outputTables) {
//                new GeoJsonWriteDriver(sql).write(new EmptyProgressVisitor(), outputTable,
//                        new File(workingDir, outputTable + ".geojson"), "UTF8", true);
//                //SHPWrite.exportTable(sql, new File(workingDir, outputTable + ".shp").getAbsolutePath(), outputTable,
//                //         ValueBoolean.TRUE);
//            }
//        }
//    }
    @Test
    public void mergeGeoJSONFilesTest() throws SQLException, IOException {
        Logger logger = LoggerFactory.getLogger("mergeGeoJSONFilesTest");
        Runtime r = Runtime.getRuntime();
        logger.info("Mem space: " + r.maxMemory());
        if(new File("/home/nicolas").exists()) {
            String workingDir = "/home/nicolas/data/plamade/dep66";
            DataSource ds = NoiseModellingInstance.createDataSource("", "", workingDir, "h2gisdb", false);
            try (Connection connection = ds.getConnection()) {
            NoiseModellingRunner.Configuration configuration = new NoiseModellingRunner.Configuration(
                    1, workingDir, 4, workingDir.substring(workingDir.length() - 2), 1,
                    null, new RootProgressVisitor(1), "build/");
                NoiseModellingRunner noiseModellingRunner = new NoiseModellingRunner(configuration, null);
                //noiseModellingRunner.makeEmission(connection);
                List<String> createdTables = NoiseModellingInstance.mergeCBS(connection, CBS_GRID_SIZE,
                        NoiseModellingInstance.CBS_MAIN_GRID_SIZE,
                        new RootProgressVisitor(1, true, 1));
                NoiseModellingRunner.exportTables(connection, createdTables,
                        new  File("out/").getAbsolutePath(), 4326);
            }
        }
    }
//
//    @Test
//    public void emissionTrainTest() throws SQLException, IOException {
//        String workingDir = "/home/nicolas/data/plamade/dep05";
//        DataSource ds = NoiseModellingRunner.createDataSource("", "",
//                workingDir, "h2gisdb", false);
//        try(Connection connection = ds.getConnection()) {
//            NoiseModellingRunner.Configuration configuration = new NoiseModellingRunner.Configuration(
//                    1, workingDir, 4, "05", 1,
//                    null, new RootProgressVisitor(1), "");
//            NoiseModellingRunner noiseModellingRunner = new NoiseModellingRunner(configuration, null);
////            NoiseModellingRunner.makeGrid(connection, 4);
////            noiseModellingRunner.makeEmission(connection);
////            NoiseModellingRunner.generateClusterConfig(connection, new EmptyProgressVisitor(), 16, workingDir);
//            NoiseModellingInstance noiseModellingInstance = new NoiseModellingInstance(connection, workingDir);
//            Main.ClusterConfiguration clusterConfiguration = Main.loadClusterConfiguration(workingDir, 0);
//
//        }
//    }
//
//    @Test
//    public void testCopy() throws IOException, SftpException {
//        //
//        NoiseModellingInstance.copyFolder(null, new EmptyProgressVisitor(),
//                new File("").getAbsolutePath().toString(), "", true);
//    }

}
