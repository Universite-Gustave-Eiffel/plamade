package org.noise_planet.covadis.scripts;


import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.Slurm.Main_Remote_Script;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        new Main_Remote_Script().filterReceivers(connection, 0, 31, 5, "RECEIVERS");
        // Check the numbers of receivers is 5000 / 32
        var rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM receivers");
        rs.next();
        assertEquals((int)(Math.ceil(5000 / 32.0)), rs.getInt(1));
    }
}
