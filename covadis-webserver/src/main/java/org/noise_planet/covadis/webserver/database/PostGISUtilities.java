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
import org.noise_planet.noisemodelling.runner.PostGISJTSDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

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
}
