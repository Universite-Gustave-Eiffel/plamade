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
import org.noise_planet.covadis.webserver.Configuration;
import org.noise_planet.covadis.webserver.script.JobStates;
import org.noise_planet.covadis.webserver.secure.JWTProviderFactory;
import org.noise_planet.covadis.webserver.secure.Role;
import org.noise_planet.covadis.webserver.secure.User;
import org.noise_planet.covadis.webserver.utilities.StringUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * Handle the creation of datasource according to application configuration
 * The Model of the Web Server
 */
public class DatabaseManagement {
    private static final int DATABASE_VERSION = 1;
    public static final String ADMIN_EMAIL = "admin@localhost";

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


    public static void initializeServerDatabaseStructure(DataSource dataSource, Configuration configuration) throws SQLException {
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
                if (databaseVersion < DATABASE_VERSION) {
                    // do upgrade
                    st.executeUpdate("UPDATE ATTRIBUTES SET DATABASE_VERSION = " + databaseVersion);
                } else if (databaseVersion > DATABASE_VERSION) {
                    throw new IllegalStateException(
                            String.format("Database more recent than application version %d > %d",
                                    databaseVersion, DATABASE_VERSION));
                }
            }
            // Check if the user database is empty
            // There should be at least the admin account in the database
            // if not create one and print the register url into the console
            if(JDBCUtilities.getRowCount(connection, "USERS") == 0) {
                // Create an admin account
                String key = addUser(connection, ADMIN_EMAIL, Role.ADMINISTRATOR, Role.RUNNER);
                Logger logger = LoggerFactory.getLogger(DatabaseManagement.class);
                logger.info("First start of the server, register the Administrator account using this url:\n" +
                        "http://localhost:{}/{}/register/{}",
                        configuration.getPort(),
                        configuration.getApplicationRootUrl(),
                        URLEncoder.encode(key, StandardCharsets.UTF_8));
            } else {
                // Check if Admin user(id:1) have not yet activating is account
                User firstAdmin = DatabaseManagement.getUser(connection, 1);
                if(firstAdmin.isAdministrator() && !firstAdmin.getRegisterToken().isEmpty()) {
                    Logger logger = LoggerFactory.getLogger(DatabaseManagement.class);
                    logger.warn("The Administrator account has not been registered yet," +
                                    " please use this url to create the account:\n" +
                                    "http://localhost:{}/{}/register/{}",
                            configuration.getPort(),
                            configuration.getApplicationRootUrl(),
                            URLEncoder.encode(firstAdmin.getRegisterToken(), StandardCharsets.UTF_8));
                }
            }
        }

    }

    private static void createServerDataBaseStructure(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        final String serverSecretToken = JWTProviderFactory.generateServerSecretToken();
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
                        "  EMAIL VARCHAR UNIQUE," +
                        "  TOTP_TOKEN VARCHAR," +
                        "  REGISTER_TOKEN VARCHAR" +
                        ")"
        );
        st.executeUpdate(
                "CREATE TABLE ROLES(" +
                        "  PK_USER INTEGER," +
                        "  ROLE VARCHAR," +
                        "  FOREIGN KEY (PK_USER) " +
                        "    REFERENCES USERS(PK_USER) " +
                        "    ON DELETE CASCADE" +
                        ")"
        );
        st.executeUpdate(
                "CREATE TABLE JOBS(" +
                        "  PK_JOB INTEGER AUTO_INCREMENT PRIMARY KEY," +
                        "  PK_USER INTEGER," +
                        "  SCRIPT_ID VARCHAR," +
                        "  PROGRESSION REAL," +
                        "  STATUS VARCHAR DEFAULT '"+ JobStates.QUEUED.name() +"'," +
                        "  BEGIN_DATE TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
                        "  END_DATE TIMESTAMP WITHOUT TIME ZONE," +
                        "  FOREIGN KEY (PK_USER) " +
                        "    REFERENCES USERS(PK_USER) " +
                        "    ON DELETE CASCADE" +
                        ")"
        );

    }

    /**
     * Retrieve the generated signing key of the server
     * That key is used to sign the JWT tokens provided to the users
     * If a malicious user tries to change the payload (ex. user identifier), then the token will not be valid
     * @param serverDataSource data source
     * @return JWTSigningKey
     * @throws SQLException Something went wrong
     */
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
     * @param connection Data source to the database
     * @param userIdentifier User identifier in the database (e.g., primary key)
     * @return List of roles associated with the user
     * @throws SQLException If something wrong happened
     */
    public static List<String> getUserRoles(Connection connection, int userIdentifier) throws SQLException {
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

    /**
     * Get user from database
     * @param serverDataSource Data source to the database
     * @param userIdentifier User identifier in the database (e.g., primary key)
     * @return User object with associated roles and details
     * @throws SQLException If something wrong happened
     */
    public static User getUser(DataSource serverDataSource, int userIdentifier) throws SQLException {
        try(Connection connection = serverDataSource.getConnection()) {
            return getUser(connection, userIdentifier);
        }
    }

    /**
     * Get user from database
     * @param connection Data source to the database
     * @param userIdentifier User identifier in the database (e.g., primary key)
     * @return User object with associated roles and details
     * @throws SQLException If something wrong happened
     */
    public static User getUser(Connection connection, int userIdentifier) throws SQLException {
        Logger logger = LoggerFactory.getLogger(DatabaseManagement.class);

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

        String registerToken = rsUser.getString("REGISTER_TOKEN");

        List<String> rolesList = getUserRoles(connection, userIdentifier);  // Retrieve the user's roles
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
        return new User(userIdentifier, email, roles, registerToken);
    }

    /**
     * This method adds a new user with the provided email.
     * <p>
     * It generates an expected token that would be provided in the url when associating to a TOTP generator
     * This token is provided to the user with the server url by mail or other communication method.
     *
     * @param connection The database Connection object used to execute the query.
     * @param email The email of the user to be added.
     * @return The token generated for this new user.
     * @throws SQLException If the operation fails to add a user (i.e., no rows are affected in the "USERS" table).
     */
    public static String addUser(Connection connection, String email, Role... roles) throws SQLException {
        String sql = "INSERT INTO USERS (EMAIL, REGISTER_TOKEN) VALUES (?, ?)";
        PreparedStatement pstUser = connection.prepareStatement(sql,
                Statement.RETURN_GENERATED_KEYS);

        // Set the parameters of the SQL statement

        String urlToken = JWTProviderFactory.generateServerSecretToken();

        pstUser.setString(1, email);
        pstUser.setString(2, urlToken);

        // Execute the statement and get the result
        int rowsAffected = pstUser.executeUpdate();

        if (rowsAffected == 0) {
            throw new SQLException("Failed to add user.");
        }

        ResultSet rs = pstUser.getGeneratedKeys();
        if (!rs.next()) {
            throw new SQLException("Failed to get user primary key.");
        }

        int userPk = rs.getInt(1);

        sql = "INSERT INTO ROLES (PK_USER, ROLE) VALUES (?, ?)";
        pstUser = connection.prepareStatement(sql);
        for(Role role : roles) {
            pstUser.setInt(1, userPk);
            pstUser.setString(2, role.toString());
            rowsAffected = pstUser.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException("Failed to add roles.");
            }
        }

        return urlToken;
    }

    /**
     * Get user by register token
     * @param connection The database Connection object
     * @param registerToken The registration token from URL
     * @return User identifier or -1 if not found
     * @throws SQLException If database error occurs
     */
    public static int getUserByRegisterToken(Connection connection, String registerToken) throws SQLException {
        String sql = "SELECT PK_USER FROM USERS WHERE REGISTER_TOKEN = ?";
        try (PreparedStatement pst = connection.prepareStatement(sql)) {
            pst.setString(1, registerToken);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("PK_USER");
                }
            }
        }
        return -1; // Token not found
    }

    /**
     * Updates the TOTP token for a user and clears their registerToken field.
     *
     * @param connection The database connection object.
     * @param userIdentifier The unique identifier of the user in the database.
     * @param totpToken The new Time-Based One-Time Password (TOTP) token to be assigned to the user.
     * @throws SQLException If there's an error executing the SQL update or if no rows were affected,
     * indicating that no user with the given identifier was found in the database.
     */
    public static void updateUserTotpToken(Connection connection, int userIdentifier, String totpToken) throws SQLException {
        // Use an UPDATE query to set the new TOTP token and clear the registerToken field for the specified user.
        String sql = "UPDATE USERS SET TOTP_TOKEN = ?, REGISTER_TOKEN = '' WHERE PK_USER = ?";
        PreparedStatement pstUser = connection.prepareStatement(sql);

        // Set the parameters of the SQL statement
        pstUser.setString(1, totpToken);
        pstUser.setInt(2, userIdentifier);

        // Execute the statement and get the result
        int rowsAffected = pstUser.executeUpdate();

        if (rowsAffected == 0) {
            throw new SQLException("Failed to update TOTP token for user with PK_USER: " + userIdentifier);
        }
    }

    /**
     * Get user by register token
     * @param connection The database Connection object
     * @param email User email
     * @return User TOTP_TOKEN or empty if not found
     * @throws SQLException If database error occurs
     */
    public static String getTotpSecretByUserEmail(Connection connection, String email) throws SQLException {
        if(email == null || email.isEmpty()) {
            return "";
        }
        String sql = "SELECT TOTP_TOKEN FROM USERS WHERE EMAIL = ?";
        try (PreparedStatement pst = connection.prepareStatement(sql)) {
            pst.setString(1, email);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("TOTP_TOKEN");
                }
            }
        }
        return ""; // email not found
    }

    /**
     * Get user id by using email
     * @param connection The database Connection object
     * @param email User email
     * @return User identifier or -1 if not found
     * @throws SQLException If database error occurs
     */
    public static int getUserIdByUserEmail(Connection connection, String email) throws SQLException {
        if(email == null || email.isEmpty()) {
            return -1;
        }
        String sql = "SELECT PK_USER FROM USERS WHERE EMAIL = ?";
        try (PreparedStatement pst = connection.prepareStatement(sql)) {
            pst.setString(1, email);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("PK_USER");
                }
            }
        }
        return -1; // email not found
    }

    /**
     * Create a new job with the specified user and return the job identifier
     * @param connection SQL Connection
     * @param userIdentifier User identifier
     * @return Job identifier
     * @throws SQLException Error
     */
    public static int createJob(Connection connection, int userIdentifier, String jobScript) throws SQLException {
        PreparedStatement st = connection.prepareStatement("INSERT INTO JOBS (PK_USER, SCRIPT_ID) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
        st.setInt(1, userIdentifier);
        st.setString(2, jobScript);
        int affectedRows = st.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Failed to create job.");
        }
        ResultSet rs = st.getGeneratedKeys();
        if (rs.next()) {
            return rs.getInt(1);
        } else  {
            throw new SQLException("Failed to create job.");
        }
    }

    /**
     * Update the state of an existing Job record.
     * <p>
     * Persists the given state in table {@code JOBS} for the row identified by {@code PK_JOB}.
     * The {@code jobState} is expected to be the name of a value from
     * {@link org.noise_planet.covadis.webserver.script.JobStates} (e.g. {@code QUEUED}, {@code RUNNING}, etc.).
     * </p>
     *
     * @param connection The open JDBC {@link Connection} to use; must not be {@code null}.
     * @param jobId The identifier of the job to update (value of column {@code PK_JOB}).
     * @param jobState The new state to set for the job (typically {@code JobStates.name()}).
     * @throws SQLException If a database access error occurs, or if no row was updated (job not found).
     */
    public static void setJobState(Connection connection, int jobId, String jobState) throws SQLException {
        PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET STATUS = ? WHERE PK_JOB = ?");
        st.setString(1, jobState);
        st.setInt(2, jobId);
        int affectedRows = st.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Failed to set job state.");
        }
    }

    public static void setJobProgression(Connection connection, int jobId, double progression) throws SQLException {
        PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET PROGRESSION = ? WHERE PK_JOB = ?");
        st.setDouble(1, progression);
        st.setInt(2, jobId);
        st.execute();
    }

    public static void setJobEndTime(Connection connection, int jobId) throws SQLException {
        PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET END_DATE = ? WHERE PK_JOB = ?");
        st.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        st.setInt(2, jobId);
        st.execute();
    }

    /**
     * Fetch the content of the JOB table
     * @param connection
     * @param filterByUserIdentifier If > 0, will filter the job for a specific user. Administrator see all jobs.
     * @return Job list
     * @throws SQLException
     */
    public static List<Map<String, Object>> getJobs(Connection connection, int filterByUserIdentifier) throws SQLException {
        List<Map<String, Object>> table = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT JOBS.*, USERS.EMAIL FROM JOBS INNER JOIN USERS ON JOBS.PK_USER = USERS.PK_USER ");
        if(filterByUserIdentifier > 0) {
            sql.append("WHERE PK_USER = ? ");
        }
        sql.append("ORDER BY BEGIN_DATE DESC");
        PreparedStatement statement = connection.prepareStatement(sql.toString());
        if(filterByUserIdentifier > 0) {
            statement.setInt(1, filterByUserIdentifier);
        }
        DecimalFormat f = (DecimalFormat)(DecimalFormat.getInstance(Locale.ROOT));
        f.applyPattern("#.### '%'");
        DateFormat mediumDateFormatEN =
                new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                Integer pkJob = rs.getInt("pk_job");
                row.put("pk_job", pkJob);
                row.put("script", rs.getString("SCRIPT_ID"));
                row.put("email", rs.getString("email"));
                Timestamp bDate = rs.getTimestamp("BEGIN_DATE");
                row.put("startDate", !rs.wasNull() ? mediumDateFormatEN.format(bDate) : "-");
                Timestamp eDate = rs.getTimestamp("END_DATE");
                String endDate = "-";
                String duration = "-";
                Duration computationTime = null;
                if(!rs.wasNull()) {
                    endDate = mediumDateFormatEN.format(eDate);
                    computationTime = Duration.ofMillis(
                            eDate.getTime()  - bDate.getTime());
                } else if(bDate != null){
                    computationTime = Duration.ofMillis(
                            System.currentTimeMillis() - bDate.getTime());
                }
                if(computationTime != null) {
                    duration = StringUtilities.durationToString(computationTime);
                }
                row.put("endDate", endDate);
                row.put("duration", duration);
                row.put("status", rs.getString("STATUS"));
                row.put("progression", f.format(rs.getDouble("PROGRESSION")));
                table.add(row);
            }
        }
        return table;
    }
}
