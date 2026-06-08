package org.noise_planet.covadis.scripts;


import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.Slurm.Main_Remote_Script;
import org.noise_planet.covadis.webserver.NoiseModellingHPCServerHttpTest;
import org.noise_planet.noisemodelling.scripts.Database_Manager.Table_Visualization_Data;
import org.noise_planet.noisemodelling.scripts.Import_and_Export.Import_File;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestScript extends JDBCTestCase {
    @Test
    public void testSlurmDeleteReceivers() throws SQLException {
        // Create receivers table with 5000 pk and empty point using batch insert
        connection.createStatement().execute("CREATE TABLE receivers (id SERIAL PRIMARY KEY, geom geometry(Point, 4326))");
        connection.setAutoCommit(false);
        var ps = connection.prepareStatement("INSERT INTO receivers (geom) VALUES (ST_GeomFromText('POINT(0 0)', 4326))");
        for (int i = 0; i < 5000; i++) {
            ps.addBatch();
        }
        ps.executeBatch();
        connection.commit();
        Main_Remote_Script.filterReceivers(connection, 0, 31, 5, "RECEIVERS");
        // Check the numbers of receivers is 5000 / 32
        var rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM receivers");
        rs.next();
        assertEquals((int)(Math.ceil(5000 / 32.0)), rs.getInt(1));
    }

    @Test
    public void testDisplayTableData() throws SQLException {
        new Import_File().exec(connection, Map.of("pathFile", NoiseModellingHPCServerHttpTest.class.getResource("receivers.shp").getFile()));
        String res = new Table_Visualization_Data().exec(connection, Map.of("tableName", "receivers")).toString();
        assertTrue(res.contains("The total number of rows is 830"));
        assertTrue(res.contains("The srid of the table is 2154"));
        assertTrue(res.contains("POINT Z(223495.9880411485 6757167.98900822 0)"));
    }
}
