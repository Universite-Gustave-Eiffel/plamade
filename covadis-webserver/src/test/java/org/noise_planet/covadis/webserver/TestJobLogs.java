/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.covadis.webserver;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.noise_planet.covadis.webserver.utilities.Logging;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJobLogs {

    @Test
    public void testExtractLogs() throws IOException {
        List<String> lastLine = Logging.getLastLines(
                new File(TestJobLogs.class.getResource("application.log").getFile()),
                10, "");
        assertEquals(10, lastLine.size());
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32));
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32));
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32));
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32));
        assertEquals("2021-08-03 17:00:56,686 INFO [re", lastLine.remove(0).substring(0, 32));
    }

    @Test
    public void testFilter() throws IOException {
        List<String> lastLine = Logging.getLastLines(
                new File(TestJobLogs.class.getResource("test.log").getFile()),
                10, "JOB_4");
        assertEquals(10, lastLine.size());
        String line = lastLine.remove(lastLine.size() - 1);
        assertEquals(" - Begin processing of cell 354 / 625", line.substring(line.lastIndexOf(" - "), line.length()));
        line = lastLine.remove(lastLine.size() - 1);
        assertEquals(" - Compute... 56.480 % (4728 receivers in this cell)",  line.substring(line.lastIndexOf(" - "), line.length()));
        line = lastLine.remove(lastLine.size() - 1);
        assertEquals(" - This computation area contains 4757 receivers 0 sound sources and 0 buildings",  line.substring(line.lastIndexOf(" - "), line.length()));

    }

}