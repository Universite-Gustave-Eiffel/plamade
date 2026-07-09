package org.noise_planet.covadis.scripts;

import com.zaxxer.hikari.HikariDataSource;
import org.h2.Driver;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.postgis_jts.ConnectionWrapper;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.noise_planet.covadis.webserver.database.PostGISUtilities;
import org.noise_planet.noisemodelling.webserver.database.DatabaseManagement;
import org.noise_planet.noisemodelling.webserver.utilities.Logging;
import org.osgi.service.jdbc.DataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public class JDBCTestCase {

    public DataSource dataSource;
    public Connection connection;
    public boolean isH2GISDatabase = false;

    /**
     * Retrieves PostgreSQL connection parameters from environment variables.
     *
     */
    public static DataSource getPostGISDatasourceFromEnv() throws SQLException {
        if(System.getenv("POSTGRES_HOST") == null) {
            return null;
        }
        String pgUser = Optional.ofNullable(System.getenv("POSTGRES_USER")).orElse("noisemodelling");
        String pgPass = Optional.ofNullable(System.getenv("POSTGRES_PASSWORD")).orElse("noisemodelling");
        String pgPort = Optional.ofNullable(System.getenv("POSTGRES_PORT")).orElse("5432");
        String pgDb = Optional.ofNullable(System.getenv("POSTGRES_DB")).orElse("noisemodelling_db");
        String pgHost = Optional.ofNullable(System.getenv("POSTGRES_HOST")).orElse("localhost");
        return PostGISUtilities.createPostgisDataSource(pgUser, pgPass, pgPort, pgDb, pgHost);
    }

    public static DataSource createDataSource(String user, String password, boolean debug) throws SQLException {
        // Create H2 memory DataSource
        Driver driver = Driver.load();
        OsgiDataSourceFactory dataSourceFactory = new OsgiDataSourceFactory(driver);
        Properties properties = new Properties();
        String databasePath = "jdbc:h2:mem:junit"+System.currentTimeMillis();
        properties.setProperty(DataSourceFactory.JDBC_URL, databasePath);
        properties.setProperty(DataSourceFactory.JDBC_USER, user);
        properties.setProperty(DataSourceFactory.JDBC_PASSWORD, password);
        if (debug) {
            properties.setProperty("TRACE_LEVEL_FILE", "3"); // enable debug
        }
        return dataSourceFactory.createDataSource(properties);
    }

    @BeforeEach
    void initConnection() throws SQLException {
        //        if(System.getenv("POSTGRES_HOST") != null) {
        //            dataSource = getPostGISParametersFromEnv();
        //        }
        dataSource = DatabaseManagement.createH2DataSource("jdbc:h2:mem:junit"+System.currentTimeMillis(), "sa", "sa", "", false);
        try(Connection rawConnection = dataSource.getConnection()) {
            DBTypes dbType = DBUtils.getDBType(rawConnection);
            isH2GISDatabase = (dbType == DBTypes.H2GIS || dbType == DBTypes.H2);
        }
        if(isH2GISDatabase) {
            connection = JDBCUtilities.wrapConnection(dataSource.getConnection());
            H2GISFunctions.load(connection);
        } else {
            connection = new ConnectionWrapper(dataSource.getConnection());
        }
    }

    @AfterEach
    void closeConnection() throws SQLException {
        connection.close();
        try {
            // close connection pool, we are supposed to have a single connection pool
            HikariDataSource hds = dataSource.unwrap(HikariDataSource.class);
            hds.close();
        } catch (SQLException e) {
            // ignore
        }
    }
    @BeforeAll
    public static void init() {
        Logging.initConsoleLogging();
    }
}
