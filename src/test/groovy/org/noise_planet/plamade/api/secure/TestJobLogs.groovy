package org.noise_planet.plamade.api.secure

import org.junit.Test

import static org.junit.jupiter.api.Assertions.assertEquals

class TestJobLogs {
    @Test
    public void testExtractLogs() throws IOException {
        List<String> lastLine = GetJobLogs.getLastLines(
                new File(TestJobLogs.class.getResource("geoserver.log").getFile()),
                10, null);
        assertEquals(10, lastLine.size());
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.pop().substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 DEBUG [e", lastLine.pop().substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.pop().substring(0, 32))
        assertEquals("2021-08-03 17:00:56,628 TRACE [u", lastLine.pop().substring(0, 32))
        assertEquals("2021-08-03 17:00:56,686 INFO [re", lastLine.pop().substring(0, 32))
    }

}
