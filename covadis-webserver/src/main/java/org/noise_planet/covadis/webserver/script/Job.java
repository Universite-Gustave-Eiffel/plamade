/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.covadis.webserver.script;

import groovy.lang.GroovyShell;
import groovy.lang.MetaMethod;
import groovy.lang.Script;
import org.h2gis.api.ProgressVisitor;
import org.jetbrains.annotations.NotNull;
import org.noise_planet.covadis.webserver.Configuration;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Manage the execution of a Groovy Script
 */
public class Job<T> implements Callable<T> {
    private static final Logger logger = LoggerFactory.getLogger(Job.class);
    /** NoiseModelling DataBase for the user */
    protected DataSource userDataSource;
    protected DataSource serverDataSource;
    protected boolean isRunning = false;
    protected int userId;
    protected int jobId;
    protected Configuration configuration;
    protected Future<T> future;
    protected ProgressVisitor progressVisitor;
    protected ExecutionPlan executionPlan;

    public Job(int userId, ExecutionPlan executionPlan,
               DataSource serverDataSource, DataSource userDataSource, Configuration configuration) throws SQLException {
        this.userId = userId;
        this.executionPlan = executionPlan;
        this.configuration = configuration;
        this.userDataSource = userDataSource;
        this.serverDataSource = serverDataSource;
        progressVisitor = new RootProgressVisitor(1, true, 5);
        try (Connection connection = serverDataSource.getConnection()) {
            this.jobId = DatabaseManagement.createJob(connection, userId, executionPlan.scriptMetadata.id);
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


    void setJobProgression(int progression) {
        try (Connection connection = serverDataSource.getConnection()) {
            DatabaseManagement.setJobProgression(connection, jobId, progression);
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
        // Change the Thread name to match the logging filter to the logging messages of this job
        Thread.currentThread().setName(getThreadName(jobId));
        isRunning = true;
        setJobState(JobStates.RUNNING);
        GroovyShell shell = new GroovyShell();
        // Follow the execution plan by executing instances of ExecutionPlan on inputs
        ExecutionPlan currentPlan = executionPlan;
        // The currentPlan is executing because the output of the currentPlan
        // is the input (parentPlanInputName) of the parent plan
        ExecutionPlan parentPlan = null;
        String parentPlanInputName = "";
        T returnData = null;
        try {
            while (currentPlan != null) {
                // Check inputs of the current plan to find the next plan to execute
                // If the input is an ExecutionPlan and not a literal value, it is the next plan to execute
                boolean recheck = false;
                for (Map.Entry<String, Object> input : currentPlan.inputs.entrySet()) {
                    if (input.getValue() instanceof ExecutionPlan) {
                        parentPlan = currentPlan;
                        parentPlanInputName = input.getKey();
                        currentPlan = (ExecutionPlan) input.getValue();
                        recheck = true;
                        break;
                    }
                }
                if (recheck) {
                    // The current plan has changed, we need to recheck the inputs
                    continue;
                }
                File scriptFile = currentPlan.scriptMetadata.path.toFile();
                Script script = shell.parse(scriptFile);
                // The script is not sandboxed so it have the same read/write access as the application
                // it is useless to try to limit access to the server configuration
                currentPlan.inputs.put("_configuration", configuration);
                // Check expected arguments
                List<MetaMethod> methods = script.getMetaClass().getMethods();
                MetaMethod execMetaMethod =
                        methods.stream().filter(m -> m.getName().equals("exec")).findFirst().orElse(null);
                boolean useConnection = true; //first argument is a connection input
                boolean useProgressVisitor = false; // third argument is a ProgressVisitor
                if (execMetaMethod != null) {
                    // 2. Access the native parameter types
                    Class[] parameterTypes = execMetaMethod.getNativeParameterTypes();
                    Class<?> firstArgClass = parameterTypes[0];
                    if (firstArgClass.equals(DataSource.class)) {
                        useConnection = false;
                    } else if (!firstArgClass.equals(Object.class) && !firstArgClass.equals(Connection.class)) {
                        throw new RuntimeException("Invalid first argument type for exec method in " + currentPlan.scriptMetadata.id);
                    }
                    if (parameterTypes.length >= 3 && parameterTypes[2].equals(ProgressVisitor.class)) {
                        useProgressVisitor = true;
                    }
                    // Exec method signature can be:
                    // def exec(Connection connection, Map input)
                    // def exec(DataSource dataSource, Map input)
                    // def exec(Connection connection, Map input, ProgressVisitor progressVisitor)
                    // def exec(DataSource dataSource, Map input, ProgressVisitor progressVisitor)
                    Object[] args = new Object[useProgressVisitor ? 3 : 2];
                    args[1] = currentPlan.inputs;
                    if (useProgressVisitor) {
                        args[2] = progressVisitor;
                    }
                    Object ret;
                    if (useConnection) {
                        // Open the connection to the database
                        try (Connection connection = userDataSource.getConnection()) {
                            args[0] = connection;
                            ret = execMetaMethod.invoke(script, args);
                        }
                    } else {
                        args[0] = userDataSource;
                        ret = execMetaMethod.invoke(script, args);
                    }
                    if (ret != null) {
                        // Unchecked cast is unavoidable due to type erasure with generics
                        // The script author is responsible for returning the correct type
                        @SuppressWarnings("unchecked") T castedReturn = (T) ret;
                        currentPlan.outputs = castedReturn;
                    }
                    setJobState(JobStates.COMPLETED);
                    setJobProgression(100);
                }
                if(parentPlan != null) {
                    // Update the value of the parent plan input
                    if(currentPlan.chainedOutputKey.isEmpty() || !(currentPlan.outputs instanceof Map)) {
                        parentPlan.inputs.put(parentPlanInputName, currentPlan.outputs);
                    } else {
                        Map<String, Object> outputs = (Map<String, Object>) currentPlan.outputs;
                        parentPlan.inputs.put(parentPlanInputName, outputs.get(currentPlan.chainedOutputKey));
                    }
                }
                currentPlan = parentPlan;
            }
        } catch (Exception ex) {
            setJobState(JobStates.FAILED);
            logger.error("Job failed", ex);
            throw new RuntimeException(ex);
        } finally {
            isRunning = false;
            onJobEnd();
        }
        return returnData;
    }

    public void cancel() {
        progressVisitor.cancel();
    }

    public boolean isRunning() {
        return isRunning;
    }

    @NotNull
    public static String getThreadName(int jobId) {
        return String.format("JOB_%d", jobId);
    }

    /**
     * @return Job id
     */
    public int getId() {
        return jobId;
    }
}
