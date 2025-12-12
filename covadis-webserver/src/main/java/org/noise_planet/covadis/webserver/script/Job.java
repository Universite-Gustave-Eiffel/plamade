/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.covadis.webserver.script;

import com.zaxxer.hikari.HikariDataSource;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.h2gis.api.ProgressVisitor;
import org.noise_planet.covadis.webserver.Configuration;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;
import org.noise_planet.covadis.webserver.secure.User;
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Manage the execution of a Groovy Script
 */
public class Job<T> implements Callable<T> {
    private static final Logger logger = LoggerFactory.getLogger(Job.class);
    private ScriptMetadata scriptMetadata;
    /** NoiseModelling DataBase for the user */
    private HikariDataSource userDataSource;
    private DataSource serverDataSource;
    private Map<String, Object> inputs;
    private boolean isRunning = false;
    private User user;
    private int jobId;
    private Configuration configuration;
    private Future<T> future;
    private ProgressVisitor progressVisitor;

    public Job(User user, ScriptMetadata scriptMetadata,
               DataSource serverDataSource, Map<String, Object> inputs, Configuration configuration) throws SQLException {
        this.user = user;
        this.scriptMetadata = scriptMetadata;
        this.configuration = configuration;
        this.userDataSource = DatabaseManagement.createH2DataSource(
                configuration.getWorkingDirectory(),
                String.format("user_%03d", user.getIdentifier()),
                "sa",
                "sa",
                "",
                true);
        this.serverDataSource = serverDataSource;
        this.inputs = inputs;
        progressVisitor = new RootProgressVisitor(1, false, 0);
        try (Connection connection = serverDataSource.getConnection()) {
            this.jobId = DatabaseManagement.createJob(connection, user.getIdentifier());
            progressVisitor.addPropertyChangeListener("PROGRESS" , new ProgressionTracker(serverDataSource, jobId));
        }
    }

    void setJobState(JobStates newState) {
        try (Connection connection = serverDataSource.getConnection()) {
            DatabaseManagement.setJobState(connection, jobId, newState.name());
        } catch (SQLException | SecurityException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
    }

    void onJobEnd() throws SQLException {
        try (Connection connection = serverDataSource.getConnection()) {
            DatabaseManagement.setJobEndTime(connection, jobId);
        } catch (SQLException | SecurityException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
    }

    public Future<T> getFuture() {
        return future;
    }

    public void setFuture(Future<T> future) {
        this.future = future;
    }

    @Override
    public T call() throws Exception {
        // Change the Thread name in order to allocate the logging messages of this job
        Thread.currentThread().setName("JOB_" + jobId);
        // Open the connection to the database
        try(Connection connection = userDataSource.getConnection()) {
            isRunning = true;
            setJobState(JobStates.RUNNING);
            GroovyShell shell = new GroovyShell();
            File scriptFile = scriptMetadata.path.toFile();
            Script script = shell.parse(scriptFile);
            Object returnData = script.invokeMethod("exec", new Object[]{connection, inputs});
            setJobState(JobStates.COMPLETED);
            return (T) returnData;
        } finally {
            isRunning = false;
            onJobEnd();
        }
    }

    /**
     * @return Job id
     */
    public int getId() {
        return jobId;
    }
}
