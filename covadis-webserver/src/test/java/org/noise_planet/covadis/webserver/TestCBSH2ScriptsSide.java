package org.noise_planet.covadis.webserver;

import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerUUEID;
import org.noise_planet.covadis.scripts.JDBCTestCase;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;

public class TestCBSH2ScriptsSide extends JDBCTestCase {

    @Test
    public void testGenerateExposureStatisticsFromFacadeExpo() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("RUNSCRIPT FROM '" + Objects.requireNonNull(TestCBSH2ScriptsSide.class.getResource("testGenerateExposureStatisticsFromFacadeExpo.sql")).getFile() + "'");
        }

        ComputePerUUEID.generateExposureStatisticsFromFacadeExpo(connection, "RD_FR_00_0781651", Map.of("078", "FR103"), "hexa");


    }
}

