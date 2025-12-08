/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2gis.functions.factory.H2GISDBFactory;
import org.h2gis.functions.factory.H2GISFunctions;
import org.h2gis.utilities.JDBCUtilities;
import org.jetbrains.annotations.NotNull;
import org.noise_planet.covadis.webserver.secure.JWTTokenProvider;
import org.noise_planet.covadis.webserver.secure.Role;
import org.noise_planet.covadis.webserver.secure.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

/**
 * Handle the creation of datasource according to application configuration
 */
public class DatabaseManagement {
    private static final int DATABASE_VERSION = 1;

    /**
     * Create H2Database datasource
     * @param databaseDirectory Where to store the database
     * @param databaseName Name of the database
     * @param userName Admin username
     * @param userPassword Admin password
     * @param secureBaseEncryptionSecret Encryption database password, optional (empty)
     * @param initializeSpatial If true initialize H2GIS
     * @return DataSource instance
     * @throws SQLException If something wrong happened
     */
    public static HikariDataSource createH2DataSource(String databaseDirectory, String databaseName, String userName,
                                                      String userPassword, String secureBaseEncryptionSecret,
                                                      boolean initializeSpatial) throws SQLException {
        HikariConfig config = new HikariConfig();

        StringBuilder connectionUrl = getConnectionUrl(databaseDirectory, databaseName,
                !secureBaseEncryptionSecret.isEmpty());

        Properties properties = new Properties();
        properties.setProperty(H2GISDBFactory.JDBC_URL, connectionUrl.toString());
        properties.setProperty(H2GISDBFactory.JDBC_USER, userName);
        properties.setProperty(H2GISDBFactory.JDBC_PASSWORD,
                secureBaseEncryptionSecret.isEmpty() ? userPassword : secureBaseEncryptionSecret + " " + userPassword);

        javax.sql.DataSource h2DataSource = H2GISDBFactory.createDataSource(properties);
        config.setDataSource(h2DataSource);
        HikariDataSource dataSource = new HikariDataSource(config);
        if (initializeSpatial) {
            // Init spatial ext
            try (Connection connection = dataSource.getConnection()) {
                H2GISFunctions.load(connection);
            }
        }
        return dataSource;
    }

    /**
     * Build H2 connection URL
     * @param databaseDirectory Database directory
     * @param databaseName Database name
     * @param databaseEncryption True to enable encryption
     * @return Connection URL
     */
    @NotNull
    private static StringBuilder getConnectionUrl(String databaseDirectory, String databaseName,
                                                  boolean databaseEncryption) {
        StringBuilder connectionUrl = new StringBuilder();
        connectionUrl.append(H2GISDBFactory.START_URL);
        try {
            connectionUrl.append(new File(databaseDirectory, databaseName).toURI().toURL());
        } catch (Exception e) {
            throw new RuntimeException("Error building H2GIS JDBC URL", e);
        }
        if (databaseEncryption) {
            connectionUrl.append(";CIPHER=AES");
        }
        return connectionUrl;
    }


    public static void initializeServerDatabaseStructure(DataSource dataSource) throws SQLException {
        try(Connection connection = dataSource.getConnection()) {
            if(!JDBCUtilities.tableExists(connection, "ATTRIBUTES")) {
                // First database
                createServerDataBaseStructure(connection);
            } else {
                // Existing database, may need to update it if old version
                int databaseVersion = DATABASE_VERSION;
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM ATTRIBUTES");
                if (rs.next()) {
                    databaseVersion = rs.getInt("DATABASE_VERSION");
                }
                // In the future check databaseVersion for database upgrades
                if (databaseVersion != DATABASE_VERSION) {

                }
            }
        }

    }

    private static void createServerDataBaseStructure(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        final String serverSecretToken = JWTTokenProvider.generateServerSecretToken();
        st.executeUpdate(
                "CREATE TABLE IF NOT EXISTS ATTRIBUTES(" +
                        "DATABASE_VERSION INTEGER," +
                        "SERVER_JWT_SIGNING_KEY VARCHAR" +
                        ")");
        PreparedStatement pst = connection.prepareStatement(
                "INSERT INTO ATTRIBUTES(" +
                        "DATABASE_VERSION," +
                        "SERVER_JWT_SIGNING_KEY)" +
                        " VALUES(?, ?);");
        pst.setInt(1, DATABASE_VERSION);
        pst.setString(2, serverSecretToken);
        pst.execute();
        st.executeUpdate(
                "CREATE TABLE USERS(" +
                        "  PK_USER SERIAL PRIMARY KEY," +
                        "  EMAIL VARCHAR," +
                        "  TOTP_TOKEN VARCHAR," +
                        "  REGISTER_TOKEN VARCHAR" +
                        ")"
        );
        st.executeUpdate(
                "CREATE TABLE ROLES(" +
                        "  PK_USER INTEGER," +
                        "  ROLE VARCHAR," +
                        "  CONSTRAINT FK_ROLES_USER " +
                        "    FOREIGN KEY (PK_USER) " +
                        "    REFERENCES USERS(PK_USER) " +
                        "    ON DELETE CASCADE" +
                        ")"
        );

    }

    public static String getJWTSigningKey(DataSource serverDataSource) throws SQLException {
        try(Connection connection = serverDataSource.getConnection()) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM ATTRIBUTES");
            if (rs.next()) {
                return rs.getString("SERVER_JWT_SIGNING_KEY");
            } else {
                throw new IllegalStateException();
            }
        }
    }

    /**
     * Get user roles
     * @param serverDataSource Data source to the database
     * @param userIdentifier User identifier in the database (e.g., primary key)
     * @return List of roles associated with the user
     * @throws SQLException If something wrong happened
     */
    public static List<String> getUserRoles(DataSource serverDataSource, int userIdentifier) throws SQLException {
        try(Connection connection = serverDataSource.getConnection())  {
            // Prepare statement to retrieve roles for a specific user
            PreparedStatement pst = connection.prepareStatement("SELECT ROLE FROM ROLES WHERE PK_USER=?");

            pst.setInt(1, userIdentifier);

            ResultSet rs = pst.executeQuery();

            // Store the roles in a list and return it
            List<String> roles = new ArrayList<>();
            while (rs.next()) {
                roles.add(rs.getString("ROLE"));
            }

            return roles;
        }
    }

    /**
     * Get user from database
     * @param serverDataSource Data source to the database
     * @param userIdentifier User identifier in the database (e.g., primary key)
     * @return User object with associated roles and details
     * @throws SQLException If something wrong happened
     */
    public static User getUser(DataSource serverDataSource, int userIdentifier) throws SQLException {
        Logger logger = LoggerFactory.getLogger(DatabaseManagement.class);

        try(Connection connection = serverDataSource.getConnection())  {
            // Prepare statement to retrieve the specific user and his roles
            String sql = "SELECT * FROM USERS WHERE PK_USER=?";
            PreparedStatement pstUser = connection.prepareStatement(sql);

            pstUser.setInt(1, userIdentifier);

            ResultSet rsUser = pstUser.executeQuery();

            // If no user found with that identifier
            if(!rsUser.next()) {
                throw new IllegalArgumentException(String.format("User %d not found", userIdentifier));
            }

            String email = rsUser.getString("EMAIL");

            List<String> rolesList = getUserRoles(serverDataSource, userIdentifier);  // Retrieve the user's roles
            List<Role> roles = new ArrayList<>();

            for (String roleName : rolesList) {
                try {
                    Role role = Role.valueOf(roleName);
                    roles.add(role);
                } catch (IllegalArgumentException ex ) {
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            }

            // Create a User object and return it
            return new User(userIdentifier, email, roles);
        }
    }

}
