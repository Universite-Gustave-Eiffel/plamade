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

        List<String> commandLines = new ArrayList<>();
        StringTokenizer s = new StringTokenizer(command, "\n");
        while(s.hasMoreTokens()) {
            commandLines.add(s.nextToken());
        }
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
