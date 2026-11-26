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
import org.h2gis.utilities.*;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.twkb.TWKBReader;
import org.noise_planet.noisemodelling.runner.PostGISJTSDataSource;
import org.postgresql.jdbc.PgResultSetMetaData;
import org.postgresql.util.PSQLException;

import javax.sql.DataSource;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        config.setMaximumPoolSize(2);
        return new HikariDataSource(config);
    }


    /**
     * Copy a PostGIS Dem table to a local h2 database table. If the target table already exist insert only new points
     *
     * @param pgConnection    PostgreSQL connection
     * @param h2Connection    H2 connection
     * @param pgTableName     PostGIS table name
     * @param h2TableName     H2 table name, if the table already exists the points will be inserted
     * @param wktFilter       Geometry to filter DEM Points to copy
     * @param progressVisitor Progress visitor
     * @param precision       The decimal digits parameters control how much precision is stored in the output
     * @param precisionZ      The decimal digits for Z coordinate precision
     */
    public static void fetchDemTable(Connection pgConnection, Connection h2Connection, String pgTableName,
                              String h2TableName, String wktFilter, ProgressVisitor progressVisitor, int precision, int precisionZ) throws SQLException, ParseException {


        String tempTableName = "TEMP_" + h2TableName + "_" + System.currentTimeMillis();
        if(!JDBCUtilities.tableExists(pgConnection, pgTableName)) {
            throw new IllegalArgumentException("Source table does not exist: " + pgTableName);
        }

        GeometryMetaData metaData =
                GeometryTableUtilities.getMetaData(pgConnection, pgTableName, "the_geom");
        // Ensure H2 table exists (Storing as GEOMETRY for H2GIS support)
        try (Statement h2Stmt = h2Connection.createStatement()) {
            h2Stmt.execute("CREATE TABLE IF NOT EXISTS %s (pk serial, the_geom %s)".formatted(h2TableName, metaData.getSQL()));
            // Remove previous spatial index
            h2Stmt.execute("DROP INDEX IF EXISTS %s_geom_idx".formatted(h2TableName));
            // Create a temporary table for bulk loading (no indexes = fast)
            h2Stmt.execute("CREATE TABLE %s (the_geom %s)".formatted(tempTableName, metaData.getSQL()));
        }

        // Count the expected number of blocks

        String countBlocksSql = String.format("SELECT COUNT(*) FROM %s WHERE ST_Intersects(the_geom, ?::geometry);",
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
        String pgSql = String.format("""
         SELECT ST_AsTWKB(ST_Collect(the_geom), ?, ?) as chunk_twkb FROM
          (SELECT the_geom, (row_number() OVER ()) / 1000 as group_id FROM %s WHERE ST_Intersects(the_geom, ?::geometry)) t
           GROUP BY group_id""", pgTableName);

        // Prepare H2 Insert query
        String h2Sql = MessageFormat.format("INSERT INTO {0} (the_geom) VALUES (?)",
                tempTableName);

        TWKBReader twkbReader = new TWKBReader();
        twkbReader.setGeometryFactory(new GeometryFactory(new PrecisionModel(), metaData.getSRID()));

        try (PreparedStatement pgPs = pgConnection.prepareStatement(pgSql); PreparedStatement h2Ps =
                h2Connection.prepareStatement(h2Sql)) {

            pgPs.setInt(1, precision);
            pgPs.setInt(2, precisionZ);
            pgPs.setString(3, wktFilter);

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


        // 4. Move unique points from Temp to Target
        // EXCEPT automatically handles internal duplicates in tempTableName
        // AND existing duplicates in h2TableName.
        String mergeSql = String.format(
                "INSERT INTO %s (the_geom) " +
                        "SELECT the_geom FROM %s " +
                        "EXCEPT " +
                        "SELECT the_geom FROM %s", h2TableName, tempTableName, h2TableName);

        try (Statement h2Stmt = h2Connection.createStatement()) {
            h2Stmt.execute(mergeSql);
            h2Stmt.execute(String.format("DROP TABLE %s", tempTableName));

            // 5. Create/Update Spatial Index
            // In H2GIS, creating an index on an existing populated table is much faster
            // than updating the index for every row during insertion.
            h2Stmt.execute("CREATE SPATIAL INDEX IF NOT EXISTS " + h2TableName + "_geom_idx ON " + h2TableName + "(the_geom)");
        }
    }

    /**
     * Description of a result set column: its name, SQL type, nullability and source table location.
     *
     * @param name          Column name
     * @param sqlType       SQL type used to build the target table
     * @param nullable      Whether the column is nullable
     * @param tableLocation Source table location of the column
     */
    public record ColumnInfo(String name, String sqlType, boolean nullable, TableLocation tableLocation) {
    }

    /**
     * Extract the column information (name, SQL type, nullability and source table location) of a result set.
     * Geometry columns are described with their geometry metadata and NUMERIC/DECIMAL columns keep their
     * precision and scale to preserve decimal values in the target database.
     *
     * @param resultSetConnection Connection linked with the result set
     * @param resultSet           Result set
     * @param targetDbType        Database type of the target database, used for dialect specific type mapping
     * @return The list of column descriptions
     * @throws SQLException if an error occurs while reading the result set metadata
     */
    public static List<ColumnInfo> getColumnInfos(Connection resultSetConnection, ResultSet resultSet, DBTypes targetDbType) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DBTypes dbType = DBUtils.getDBType(resultSetConnection);
        int columnCount = resultSetMetaData.getColumnCount();
        List<ColumnInfo> columnInfos = new ArrayList<>();

        for (int i = 1; i <= columnCount; i++) {
            boolean nullable = resultSetMetaData.isNullable(i) != ResultSetMetaData.columnNoNulls;
            String name = TableLocation.capsIdentifier(resultSetMetaData.getColumnLabel(i), dbType);

            TableLocation tableLocation;
            if (resultSetMetaData.isWrapperFor(PgResultSetMetaData.class)) {
                PgResultSetMetaData pgResultSetMetaData = resultSetMetaData.unwrap(PgResultSetMetaData.class);
                tableLocation = new TableLocation(pgResultSetMetaData.getBaseSchemaName(i),
                        pgResultSetMetaData.getBaseTableName(i), DBTypes.POSTGIS);
            } else {
                tableLocation = new TableLocation(resultSetMetaData.getSchemaName(i),
                        resultSetMetaData.getTableName(i), dbType);
            }

            int dataType = resultSetMetaData.getColumnType(i);
            String typeName = resultSetMetaData.getColumnTypeName(i);
            String sqlType;

            // 1. Handle Geometry columns
            if (typeName.toLowerCase(Locale.ROOT).contains("geometry")) {
                GeometryMetaData metaData =
                        GeometryTableUtilities.getMetaData(resultSetConnection, tableLocation, resultSetMetaData.getColumnLabel(i));
                sqlType = metaData != null ? metaData.getSQL() : typeName;
                // 2. Handle NUMERIC / DECIMAL to preserve decimal scale
            } else if (dataType == Types.NUMERIC || dataType == Types.DECIMAL) {
                int precision = resultSetMetaData.getPrecision(i);
                int scale = resultSetMetaData.getScale(i);

                if (precision > 0 && scale > 0) {
                    sqlType = typeName + "(" + precision + ", " + scale + ")";
                } else if (precision > 0 && scale == 0) {
                    sqlType = typeName + "(" + precision + ", 0)";
                } else if (targetDbType == DBTypes.POSTGIS || targetDbType == DBTypes.POSTGRESQL) {
                    sqlType = typeName;
                } else {
                    // Unconstrained NUMERIC (e.g. Postgres NUMERIC) defaults to scale 0 in H2.
                    // DOUBLE PRECISION avoids the integer truncation.
                    sqlType = "DOUBLE PRECISION";
                }
                // 3. Other column types
            } else {
                sqlType = typeName;
            }
            columnInfos.add(new ColumnInfo(name, sqlType, nullable, tableLocation));
        }
        return columnInfos;
    }

    /**
     * Build the CREATE TABLE SQL statement for the given table name and column descriptions.
     *
     * @param tableName Target table name
     * @param columns   Column descriptions
     * @return The CREATE TABLE SQL statement
     */
    public static String createTableSql(String tableName, List<ColumnInfo> columns) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(tableName).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            createTableSql.append(column.name()).append(" ").append(column.sqlType());
            if (!column.nullable()) {
                createTableSql.append(" NOT NULL");
            }
            if (i < columns.size() - 1) {
                createTableSql.append(", ");
            }
        }
        createTableSql.append(")");
        return createTableSql.toString();
    }

    /**
     * Copy a result set content to a target database.
     *
     * @param resultSetConnection Connection linked with the result set
     * @param resultSet           Result set (caller is responsible to close it)
     * @param targetConnection    Connection where to store the data
     * @param targetTableName     Name of the target table
     * @param dropTargetTable     Whether to drop the target table before copying
     */
    public static void copyResultSetToDatabase(Connection resultSetConnection, ResultSet resultSet, Connection targetConnection,
                                               String targetTableName, boolean dropTargetTable, int batchSize) throws SQLException {
        // Collect ResultSet metadata
        DBTypes targetDbType = DBUtils.getDBType(targetConnection);
        List<ColumnInfo> columnInfos = getColumnInfos(resultSetConnection, resultSet, targetDbType);
        List<TableLocation> fieldTableLocations = columnInfos.stream().map(ColumnInfo::tableLocation).toList();

        // Drop and Create target table
        if (!JDBCUtilities.tableExists(targetConnection, targetTableName) || dropTargetTable) {
            if (dropTargetTable) {
                try (Statement targetConnectionStatement = targetConnection.createStatement()) {
                    targetConnectionStatement.execute("DROP TABLE IF EXISTS " + targetTableName);
                }
            }
            String createTableSql = createTableSql(targetTableName, columnInfos);
            try (Statement targetConnectionStatement = targetConnection.createStatement()) {
                targetConnectionStatement.execute(createTableSql);
            } catch (SQLException e) {
                rethrowException(e, createTableSql);
            }
        }

        // Insert values using batch
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(targetTableName).append(" VALUES (");
        for (int i = 0; i < columnInfos.size(); i++) {
            insertSql.append("?");
            if (i < columnInfos.size() - 1) {
                insertSql.append(", ");
            }
        }
        insertSql.append(")");

        try (PreparedStatement ps = targetConnection.prepareStatement(insertSql.toString())) {
            int count = 0;
            boolean nonPushedBatch = false;
            while (resultSet.next()) {
                for (int i = 1; i <= columnInfos.size(); i++) {
                    final Object value = resultSet.getObject(i);
                    ps.setObject(i, value);
                }
                ps.addBatch();
                nonPushedBatch = true;
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    nonPushedBatch = false;
                }
            }
            if (nonPushedBatch) {
                ps.executeBatch();
            }
        }

        // Add primary key if applicable
        if (!fieldTableLocations.isEmpty() && !fieldTableLocations.getFirst().getTable().isEmpty() &&
                fieldTableLocations.stream().allMatch(t -> t.equals(fieldTableLocations.getFirst()))) {
            Tuple<String, Integer> pkInfo = JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(resultSetConnection, fieldTableLocations.getFirst());
            if (pkInfo != null && columnInfos.stream().anyMatch(c -> c.name().equalsIgnoreCase(pkInfo.first()))) {
                String pkName = pkInfo.first();
                try (Statement h2Stmt = targetConnection.createStatement()) {
                    h2Stmt.execute("ALTER TABLE " + targetTableName + " ADD PRIMARY KEY (" + pkName + ")");
                }
            }
        }
    }

    /**
     * Format error message with location of the error if possible then rethrow the exception
     * @param e SQL exception
     * @param sqlQuery SQL query related to the error
     * @throws SQLException Sql exception with more details
     */
    public static void rethrowException(SQLException e, String sqlQuery) throws SQLException {
        if(e instanceof PSQLException psqlException && psqlException.getServerErrorMessage() != null) {
            String sqlWithErrorMarker = sqlQuery.substring(0,
                    psqlException.getServerErrorMessage().getPosition()) + "[*]" +
                    sqlQuery.substring(psqlException.getServerErrorMessage().getPosition());

            throw new SQLException(MessageFormat.format("Error while executing this query {0}",
                    sqlWithErrorMarker), e);
        } else {
            throw new SQLException(MessageFormat.format("Error while executing this query {0}", sqlQuery), e);
        }
    }
}
