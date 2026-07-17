/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and
 * education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.webserver.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2gis.api.ProgressVisitor;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.twkb.TWKBReader;
import org.noise_planet.noisemodelling.runner.PostGISJTSDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.text.MessageFormat;

public class PostGISUtilities {

    /**
     * Creates a DataSource for PostgreSQL using the provided parameters.
     * @param user Database username
     * @param password Password associated with username
     * @param port Port number ex: 5432
     * @param database DataBase name
     * @param host Host address
     * @return a DataSource configured for PostgreSQL
     * @throws java.sql.SQLException if an error occurs while creating the DataSource
     */
    public static DataSource createPostgisDataSource(String user, String password, String port, String database, String host) throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setUsername(user);
        config.setPassword(password);
        config.setDataSourceClassName(PostGISJTSDataSource.class.getCanonicalName());
        config.addDataSourceProperty("portNumbers", Integer.parseInt(port));
        config.addDataSourceProperty("databaseName", database);
        config.addDataSourceProperty("serverNames", host);
        return new HikariDataSource(config);
    }


    /**
     * Copy a PostGIS Dem table to a local h2 database table
     *
     * @param pgConnection    PostgreSQL connection
     * @param h2Connection    H2 connection
     * @param pgTableName     PostGIS table name
     * @param h2TableName     H2 table name, if the table already exists the points will be inserted
     * @param wktFilter       Geometry to filter DEM Points to copy
     * @param progressVisitor Progress visitor
     */
    public static void fetchDemTable(Connection pgConnection, Connection h2Connection, String pgTableName,
                              String h2TableName, String wktFilter, ProgressVisitor progressVisitor) throws SQLException, ParseException {

        TWKBReader twkbReader = new TWKBReader();

        // Ensure H2 table exists (Storing as GEOMETRY for H2GIS support)
        try (Statement h2Stmt = h2Connection.createStatement()) {
            h2Stmt.execute("CREATE TABLE IF NOT EXISTS " + h2TableName + " (the_geom GEOMETRY)");
            // Create a spatial index to make the duplicate check efficient
            h2Stmt.execute("CREATE SPATIAL INDEX IF NOT EXISTS " + h2TableName + "_geom_idx ON " + h2TableName +
                    "(the_geom)");
        }

        // Count the expected number of blocks

        String countBlocksSql = String.format("SELECT COUNT(*) FROM %s WHERE the_geom && ?::geometry;",
                pgTableName);
        int expectedBlocks = 0;
        try (PreparedStatement ps = pgConnection.prepareStatement(countBlocksSql)) {
            ps.setString(1, wktFilter);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    expectedBlocks = rs.getInt(1) / 1000;
                }
            }
        }

        ProgressVisitor subProcess = progressVisitor.subProcess(expectedBlocks);

        // Prepare PostGIS query (Grouping points into TWKB chunks)
        String pgSql = String.format("SELECT ST_AsTWKB(ST_Collect(the_geom), 1) as chunk_twkb " + "FROM (" + "   " +
                " SELECT the_geom, (row_number() OVER ()) / 1000 as group_id " + "    FROM %s " + "    WHERE " +
                "the_geom && ?::geometry" + ") t " + "GROUP BY group_id", pgTableName);

        // Prepare H2 Insert query
        // Using "WHERE NOT EXISTS" to prevent duplicates based on geometry
        String h2Sql = MessageFormat.format("INSERT INTO {0} (the_geom) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM {1} WHERE the_geom = ?)",
                h2TableName,
                h2TableName);

        try (PreparedStatement pgPs = pgConnection.prepareStatement(pgSql); PreparedStatement h2Ps =
                h2Connection.prepareStatement(h2Sql)) {

            pgPs.setString(1, wktFilter);

            try (ResultSet rs = pgPs.executeQuery()) {
                int batchSize = 0;

                while (rs.next()) {
                    byte[] twkbBytes = rs.getBytes("chunk_twkb");
                    if (twkbBytes == null) continue;

                    // Decode chunk (MultiPoint because of ST_Collect)
                    Geometry decoded = twkbReader.read(twkbBytes);

                    if (decoded instanceof MultiPoint multiPoint) {
                        for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
                            Point p = (Point) multiPoint.getGeometryN(i);

                            // H2 requires the geometry object (JTS Point works with H2 driver)
                            h2Ps.setObject(1, p);
                            h2Ps.setObject(2, p);
                            h2Ps.addBatch();
                            batchSize++;

                            // Execute batch every 1000 sub-points
                            if (batchSize % 1000 == 0) {
                                h2Ps.executeBatch();
                            }
                        }
                    }
                    subProcess.endStep();
                }
                h2Ps.executeBatch(); // Final flush
            }
        }
    }
}
