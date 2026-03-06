package org.noise_planet.covadis.scripts;


import org.h2gis.api.EmptyProgressVisitor;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.Slurm.FilterTaskReceivers;

import java.sql.SQLException;
import java.util.Map;

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
        new FilterTaskReceivers().exec(connection, Map.of("taskId", 5, "minTaskId" , 0, "maxTaskId" , 31, "tableReceivers", "RECEIVERS"), new EmptyProgressVisitor());
        // Check the numbers of receivers is 5000 / 32
        var rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM receivers");
        rs.next();
        assertEquals((int)(Math.ceil(5000 / 32.0)), rs.getInt(1));
    }
}
