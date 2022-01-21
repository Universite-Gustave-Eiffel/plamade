package org.noise_planet.plamade.api.secure

import org.junit.Test

import static org.junit.Assert.assertEquals

class TestJobLogs {
    @Test
    void testExtractLogs() throws IOException {
        List<String> lastLine = GetJobLogs.getLastLines(
                new File(TestJobLogs.class.getResource("geoserver.log").getFile()),
                10);
        assertEquals(10, lastLine.size());
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.remove(0).substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.remove(0).substring(0, 32))
        assertEquals("2021-08-03 17:00:56,686 INFO [re", lastLine.remove(0).substring(0, 32))
    }

    @Test
    void testFilter() throws IOException {
        List<String> lastLine = GetJobLogs.getLastLines(
                new File(TestJobLogs.class.getResource("test.log").getFile()),
                10);
        assertEquals(10, lastLine.size());
        lastLine = GetJobLogs.filterByThread(lastLine, "JOB_4")
        assertEquals(6, lastLine.size());
        String line = lastLine.remove(lastLine.size() - 1)
        assertEquals(" - Begin processing of cell 354 / 625", line.substring(line.lastIndexOf(" - "), line.length()))
        line = lastLine.remove(lastLine.size() - 1)
        assertEquals(" - Compute... 56.480 % (4728 receivers in this cell)",  line.substring(line.lastIndexOf(" - "), line.length()))
        line = lastLine.remove(lastLine.size() - 1)
        assertEquals(" - This computation area contains 4757 receivers 0 sound sources and 0 buildings",  line.substring(line.lastIndexOf(" - "), line.length()))

    }
}
