package org.noise_planet.covadis.webserver;

import com.zaxxer.hikari.HikariDataSource;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.JDBCUtilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ExposeDEMforUniqueUUEID;
import org.noise_planet.noisemodelling.webserver.database.DatabaseManagement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for ExposeDEMforUniqueUUEID.generateContours: extraction of the contour lines
 * from a DEM points table using the H2GIS triangulation and contouring functions.
 */
public class TestExposeDEMforUniqueUUEID {

    DataSource dataSource;
    Connection connection;

    private void openConnection() throws SQLException {
        closeConnection();
        dataSource = DatabaseManagement.createH2DataSource("jdbc:h2:mem:junit" + System.nanoTime(), "sa", "sa", "", false);
        connection = JDBCUtilities.wrapConnection(dataSource.getConnection());
        H2GISFunctions.load(connection);
    }

    @AfterEach
    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (dataSource != null) {
            try {
                dataSource.unwrap(HikariDataSource.class).close();
            } catch (SQLException ex) {
                // ignore
            }
            dataSource = null;
        }
    }

    private void createSlopedPlaneDem() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE DEM_ENRICHED (PK SERIAL, THE_GEOM GEOMETRY(POINT Z, 2154))");
            // Sloped plane z = x, the contour lines every 1 m must follow the y axis at x = level
            for (int x = 0; x <= 10; x += 2) {
                for (int y = 0; y <= 10; y += 2) {
                    stmt.execute(String.format("INSERT INTO DEM_ENRICHED (THE_GEOM) VALUES (ST_GeomFromText('POINT Z (%d %d %d)', 2154))", x, y, x));
                }
            }
        }
    }

    private void assertContours(int[] expectedLevels) throws SQLException {
        try(Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT IDISO, ST_ZMIN(THE_GEOM) ZMIN, ST_ZMAX(THE_GEOM) ZMAX, COUNT(*) CPT FROM DEM_CONTOURS GROUP BY IDISO ORDER BY IDISO")) {
            int i = 0;
            while (rs.next()) {
                assertEquals(expectedLevels[i], rs.getInt("IDISO"), "Unexpected contour level");
                assertEquals(expectedLevels[i], rs.getDouble("ZMIN"), 1e-4, "Contour line is not horizontal");
                assertEquals(expectedLevels[i], rs.getDouble("ZMAX"), 1e-4, "Contour line is not horizontal");
                assertEquals(1, rs.getInt("CPT"), "The contour line is fragmented");
                i++;
            }
            assertEquals(expectedLevels.length, i, "Unexpected number of contour lines");
        }
    }

    @Test
    public void testGenerateContours() throws SQLException {
        openConnection();
        createSlopedPlaneDem();

        // One contour line for each level crossed by the plane z = x
        assertEquals(10, ExposeDEMforUniqueUUEID.generateContours(connection, "DEM_ENRICHED", 1.0));

        assertContours(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    }

    @Test
    public void testGenerateContoursCustomInterval() throws SQLException {
        openConnection();
        createSlopedPlaneDem();

        assertEquals(5, ExposeDEMforUniqueUUEID.generateContours(connection, "DEM_ENRICHED", 2.0));

        assertContours(new int[] {0, 2, 4, 6, 8});
    }

    @Test
    public void testGenerateContoursOnFlatDem() throws SQLException {
        openConnection();
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE DEM_ENRICHED (PK SERIAL, THE_GEOM GEOMETRY(POINT Z, 2154))");
            for (int x = 0; x <= 10; x += 2) {
                for (int y = 0; y <= 10; y += 2) {
                    stmt.execute(String.format("INSERT INTO DEM_ENRICHED (THE_GEOM) VALUES (ST_GeomFromText('POINT Z (%d %d 5)', 2154))", x, y));
                }
            }
        }

        assertThrows(IllegalArgumentException.class,
                () -> ExposeDEMforUniqueUUEID.generateContours(connection, "DEM_ENRICHED", 1.0));
    }

    @Test
    public void testGenerateContoursOnEmptyDem() throws SQLException {
        openConnection();
        try(Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE DEM_ENRICHED (PK SERIAL, THE_GEOM GEOMETRY(POINT Z, 2154))");
        }

        assertThrows(IllegalArgumentException.class,
                () -> ExposeDEMforUniqueUUEID.generateContours(connection, "DEM_ENRICHED", 1.0));
    }
}
