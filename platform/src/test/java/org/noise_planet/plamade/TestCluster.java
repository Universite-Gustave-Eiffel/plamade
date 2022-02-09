package org.noise_planet.plamade;

import com.jcraft.jsch.SftpException;
import org.h2gis.api.EmptyProgressVisitor;
import org.junit.Test;
import org.noise_planet.plamade.process.NoiseModellingInstance;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static junit.framework.TestCase.assertEquals;

public class TestCluster {

    public static List<String> splitCommand(String command) {
        List<String> commandLines = new ArrayList<>();
        StringTokenizer s = new StringTokenizer(command, "\n");
        while(s.hasMoreTokens()) {
            commandLines.add(s.nextToken());
        }
        return commandLines;
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
        List<NoiseModellingInstance.FileAttributes> files = NoiseModellingInstance.parseLSCommand(commandLines);
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
        String command = "       JobID    JobName               Start                 End    Elapsed        NodeList " +
                "     State ExitCode   TotalCPU \n" + "------------ ---------- ------------------- " +
                "------------------- ---------- --------------- ---------- -------- ---------- \n14904827_0   " +
                "noisemode+ 2022-01-27T13:10:08 2022-01-27T13:22:15   00:12:07        hpc-n867  COMPLETED      0:0   " +
                "04:34:53 \n" + "14904827_0.+      batch 2022-01-27T13:10:08 2022-01-27T13:22:15   00:12:07        " +
                "hpc-n867  COMPLETED      0:0   04:34:53 \n" + "14904827_1   noisemode+ 2022-01-27T13:10:08          " +
                "   Unknown   00:18:48        hpc-n857    RUNNING      0:0   00:00:00 \n" + "14904827_2   noisemode+ " +
                "2022-01-27T13:10:08 2022-01-27T13:19:37   00:09:29        hpc-n821  COMPLETED      0:0   02:09:34 " +
                "\n" + "14904827_2.+      batch 2022-01-27T13:10:08 2022-01-27T13:19:37   00:09:29        hpc-n821  " +
                "COMPLETED      0:0   02:09:34 \n" + "14904827_3   noisemode+ 2022-01-27T13:10:08 2022-01-27T13:12:44" +
                "   00:02:36        hpc-n838  COMPLETED      0:0  37:09.578 \n" + "14904827_3.+      batch " +
                "2022-01-27T13:10:08 2022-01-27T13:12:44   00:02:36        hpc-n838  COMPLETED      0:0  37:09.578 " +
                "\n" + "14904827_4   noisemode+ 2022-01-27T13:10:08 2022-01-27T13:14:00   00:03:52        hpc-n892  " +
                "COMPLETED      0:0   01:36:13 \n" + "14904827_4.+      batch 2022-01-27T13:10:08 2022-01-27T13:14:00" +
                "   00:03:52        hpc-n892  COMPLETED      0:0   01:36:13 \n" + "14904827_5   noisemode+ " +
                "2022-01-27T13:10:08 2022-01-27T13:11:34   00:01:26        hpc-n893  COMPLETED      0:0  24:47.312 " +
                "\n" + "14904827_5.+      batch 2022-01-27T13:10:08 2022-01-27T13:11:34   00:01:26        hpc-n893  " +
                "COMPLETED      0:0  24:47.312 \n" + "14904827_6   noisemode+ 2022-01-27T13:10:08 2022-01-27T13:21:07" +
                "   00:10:59        hpc-n859  COMPLETED      0:0   03:34:59 \n" + "14904827_6.+      batch " +
                "2022-01-27T13:10:08 2022-01-27T13:21:07   00:10:59        hpc-n859  COMPLETED      0:0   03:34:59 " +
                "\n" + "14904827_7   noisemode+ 2022-01-27T13:10:08 2022-01-27T13:17:12   00:07:04        hpc-n860  " +
                "COMPLETED      0:0   02:02:55 \n" + "14904827_7.+      batch 2022-01-27T13:10:08 2022-01-27T13:17:12" +
                "   00:07:04        hpc-n860  COMPLETED      0:0   02:02:55 \n" + "14904827_8   noisemode+ " +
                "2022-01-27T13:10:08 2022-01-27T13:16:46   00:06:38        hpc-n861  COMPLETED      0:0   01:53:33 " +
                "\n" + "14904827_8.+      batch 2022-01-27T13:10:08 2022-01-27T13:16:46   00:06:38        hpc-n861  " +
                "COMPLETED      0:0   01:53:33 \n" + "14904827_9   noisemode+ 2022-01-27T13:10:08 2022-01-27T13:18:28" +
                "   00:08:20        hpc-n774  COMPLETED      0:0   02:20:29 \n" + "14904827_9.+      batch " +
                "2022-01-27T13:10:08 2022-01-27T13:18:28   00:08:20        hpc-n774  COMPLETED      0:0   02:20:29 " +
                "\n" + "14904827_10  noisemode+ 2022-01-27T13:10:08 2022-01-27T13:15:42   00:05:34        hpc-n775  " +
                "COMPLETED      0:0   01:29:42 \n" + "14904827_10+      batch 2022-01-27T13:10:08 2022-01-27T13:15:42" +
                "   00:05:34        hpc-n775  COMPLETED      0:0   01:29:42";

        List<String> commandLines = splitCommand(command);
        List<NoiseModellingInstance.SlurmJobStatus> jobList = NoiseModellingInstance.parseSlurmStatus(commandLines, 14904827);
        assertEquals(11, jobList.size());
        assertEquals("RUNNING", jobList.get(1).status);
        assertEquals(1, jobList.get(1).taskId);
        assertEquals("COMPLETED", jobList.get(2).status);
        assertEquals(2, jobList.get(2).taskId);
        //,
        //                14904827
    }
//    @Test
//    public void mergeShapeFilesTest() throws SQLException, IOException {
//        String workingDir = "/home/nicolas/data/plamade/dep44/results/job_14770791";
//        DataSource ds = NoiseModellingInstance.createDataSource("", "",
//                "build", "h2gisdb", false);
//        try(Connection sql = ds.getConnection()) {
//            NoiseModellingInstance.mergeShapeFiles(sql, workingDir, "out_", "_");
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
