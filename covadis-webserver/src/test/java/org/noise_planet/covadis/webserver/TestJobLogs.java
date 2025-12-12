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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TestJobLogs {

    /**
     * Asserts that the actual string contains in expected full string.
     * This method is intended for use with JUnit 5 and its assertions,
     * and it uses the JUnit 5's assertTrue() to perform the assertion.
     *
     * @param expectedFullString The expected full string that should contain actualContainTest.
     * @param actualContainTest The string to check for in the expectedFullString.
     */
    public static void assertContains(String expectedFullString, String actualContainTest) {
        Assertions.assertTrue(expectedFullString.contains(actualContainTest),
                () -> "The actual contain test: '" + actualContainTest
                        + "' is not found in the expected full string: '" + expectedFullString + "'");
    }

    /**
     * Check if the most recent logging rows have been fetched by this method
     * @throws IOException
     */
    @Test
    public void testFilter() throws IOException, URISyntaxException {
        AtomicInteger fetchedLines = new AtomicInteger(0);
        URL resourceFile = TestJobLogs.class.getResource("test.log");
        assertNotNull(resourceFile);
        String lastLines = Logging.getLastLines(
                new File(resourceFile.toURI()),
                -1, "JOB_4", fetchedLines);
        assertEquals(32, fetchedLines.get());
        assertLinesMatch(Arrays.asList("[JOB_4] INFO  org.noise_planet.noisemodelling.jdbc.PointNoiseMap  - Begin " +
                "processing of cell 354 / 625", "[JOB_4] INFO  org.noise_planet.noisemodelling  - Compute... 56.480 %" +
                " (4728 receivers in this cell)", "[JOB_4] INFO  org.noise_planet.noisemodelling  - This computation " +
                "area contains 4757 receivers 0 sound sources and 0 buildings", "[JOB_4] INFO  org.noise_planet" +
                ".noisemodelling.jdbc.PointNoiseMap  - This computation area contains 4757 receivers 0 sound sources " +
                "and 0 buildings"), Arrays.asList(Arrays.copyOfRange(lastLines.split("\n"), 0, 4)));
    }

    /**
     * Check if the most recent logging rows have been fetched by this method
     * @throws IOException
     */
    @Test
    public void testFilterLimited() throws IOException, URISyntaxException {
        AtomicInteger fetchedLines = new AtomicInteger(0);
        URL resourceFile = TestJobLogs.class.getResource("test.log");
        assertNotNull(resourceFile);
        String lastLines = Logging.getLastLines(
                new File(resourceFile.toURI()),
                4, "JOB_4", fetchedLines);
        assertEquals(4, fetchedLines.get());
        assertLinesMatch(Arrays.asList("[JOB_4] INFO  org.noise_planet.noisemodelling.jdbc.PointNoiseMap  - Begin " +
                "processing of cell 354 / 625", "[JOB_4] INFO  org.noise_planet.noisemodelling  - Compute... 56.480 %" +
                " (4728 receivers in this cell)", "[JOB_4] INFO  org.noise_planet.noisemodelling  - This computation " +
                "area contains 4757 receivers 0 sound sources and 0 buildings", "[JOB_4] INFO  org.noise_planet" +
                ".noisemodelling.jdbc.PointNoiseMap  - This computation area contains 4757 receivers 0 sound sources " +
                "and 0 buildings"), Arrays.asList(lastLines.split("\n")));
    }
}