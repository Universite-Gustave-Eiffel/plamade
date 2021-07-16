/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.plamade.process;

import org.h2.Driver;
import org.h2.util.OsgiDataSourceFactory;
import org.h2gis.functions.factory.H2GISFunctions;
import org.osgi.service.jdbc.DataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * This process hold one instance of NoiseModelling
 * @author Nicolas Fortin, Université Gustave Eiffel
 */
public class NoiseModellingInstance implements RunnableFuture<String> {
    Logger logger = LoggerFactory.getLogger(NoiseModellingInstance.class);
    Configuration configuration;
    DataSource nmDataSource;
    boolean isRunning = false;
    boolean isCanceled = false;

    public NoiseModellingInstance(Configuration configuration) {
        this.configuration = configuration;
    }

    public static DataSource createDataSource(Configuration configuration, String user, String password, String dbName) throws SQLException {
        // Create H2 memory DataSource
        org.h2.Driver driver = org.h2.Driver.load();
        OsgiDataSourceFactory dataSourceFactory = new OsgiDataSourceFactory(driver);
        Properties properties = new Properties();
        String databasePath = "jdbc:h2:" + new File(configuration.getWorkingDirectory(), dbName).getAbsolutePath();
        properties.setProperty(DataSourceFactory.JDBC_URL, databasePath);
        properties.setProperty(DataSourceFactory.JDBC_USER, user);
        properties.setProperty(DataSourceFactory.JDBC_PASSWORD, password);
        DataSource dataSource = dataSourceFactory.createDataSource(properties);
        // Init spatial ext
        try (Connection connection = dataSource.getConnection()) {
            H2GISFunctions.load(connection);
        }
        return dataSource;

    }

    @Override
    public void run() {
        isRunning = true;
        try {
            // create folder
            if(!(new File(configuration.workingDirectory).mkdirs())) {
                logger.error("Cannot create the working directory\n" +configuration.workingDirectory);
                return;
            }
            nmDataSource = createDataSource(configuration, "sa", "sa", "nm_db");
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        } finally {
            isRunning = false;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        isCanceled = true;
        return false;
    }

    @Override
    public boolean isCancelled() {
        return isCanceled;
    }

    @Override
    public boolean isDone() {
        return !isRunning;
    }

    @Override
    public String get() throws InterruptedException, ExecutionException {
        return "";
    }

    @Override
    public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return "";
    }

    public static class Configuration {
        private DataSource dataSource;
        private String workingDirectory;
        private int configurationId;
        private String inseeDepartment;

        public Configuration(DataSource dataSource, String workingDirectory, int configurationId,
                             String inseeDepartment) {
            this.dataSource = dataSource;
            this.workingDirectory = workingDirectory;
            this.configurationId = configurationId;
            this.inseeDepartment = inseeDepartment;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public String getWorkingDirectory() {
            return workingDirectory;
        }

        public int getConfigurationId() {
            return configurationId;
        }

        public String getInseeDepartment() {
            return inseeDepartment;
        }
    }
}