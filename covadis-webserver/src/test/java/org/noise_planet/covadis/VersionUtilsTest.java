package org.noise_planet.covadis;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class VersionUtilsTest {

    @Test
    public void testGetVersion() {
        String version = VersionUtils.getVersion();
        assertNotNull(version);
        assertNotEquals("Unknown", version);
    }
}
