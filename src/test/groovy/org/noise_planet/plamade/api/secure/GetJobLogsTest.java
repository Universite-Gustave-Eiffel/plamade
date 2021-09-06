package org.noise_planet.plamade.api.secure;

import org.junit.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GetJobLogsTest {

    @Test
    void testExtractLogs() {
        List<String> lastLine = GetJobLogs.getLastLines();
    }
}