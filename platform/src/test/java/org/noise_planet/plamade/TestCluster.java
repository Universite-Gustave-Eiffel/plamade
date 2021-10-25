package org.noise_planet.plamade;

import org.junit.Test;
import org.noise_planet.plamade.process.NoiseModellingInstance;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class TestCluster {

    @Test
    public void mergeShapeFilesTest() throws SQLException, IOException {
        String workingDir = "/home/nicolas/data/plamade/dep44/results/job_14770791";
        DataSource ds = NoiseModellingInstance.createDataSource("", "",
                "build", "h2gisdb", false);
        try(Connection sql = ds.getConnection()) {
            NoiseModellingInstance.mergeShapeFiles(sql, workingDir, "out_", "_");
        }
    }
}
