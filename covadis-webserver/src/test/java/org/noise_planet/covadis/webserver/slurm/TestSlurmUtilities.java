/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and
 * education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.webserver.slurm;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestSlurmUtilities {
    /**
     * Parsing of the output of the command "scontrol show job xxx"
     */
    @Test
    public void testParseSControl() throws IOException {
        URL url = TestSlurmUtilities.class.getResource("scontrol_output.txt");
        assertNotNull(url);
        try(FileReader reader = new FileReader(url.getFile())) {
            List<String> lines = IOUtils.readLines(reader);
            List<SlurmJobStatus> jobStatusList = SlurmUtilities.parseSlurmStatus(lines);
            assertNotNull(jobStatusList);
            assertEquals(8, jobStatusList.size());
            // sort by jobstatus.taskId
            jobStatusList.sort((a, b) -> Integer.compare(a.taskId, b.taskId));
            assertEquals("RUNNING", jobStatusList.get(0).status);
            assertEquals("COMPLETED", jobStatusList.get(1).status);
            assertEquals("RUNNING", jobStatusList.get(2).status);
        }
    }
}
