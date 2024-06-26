/*
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user-friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/*
  @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.plamade.api.secure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import org.noise_planet.noisemodelling.pathfinder.RootProgressVisitor;
import org.noise_planet.plamade.config.DataBaseConfig;
import org.noise_planet.plamade.config.SlurmConfigRoot;
import org.noise_planet.plamade.process.JobExecutorService;
import org.noise_planet.plamade.process.NoiseModellingRunner;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.form.Form;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import javax.sql.DataSource;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Create new job
 */
public class PostAddJob implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(PostAddJob.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                final Map<String, Object> model = Maps.newHashMap();
                final String inseeDepartment = f.get("INSEE_DEPARTMENT");
                final String confId = f.get("CONF_ID");
                final boolean computeOnCluster = Boolean.parseBoolean(f.getOrDefault(
                        "CLUSTER_COMPUTE", Boolean.FALSE.toString()));
                if(inseeDepartment == null || inseeDepartment.equals("")) {
                    model.put("message", "Missing required field inseeDepartment");
                    ctx.render(Template.thymeleafTemplate(model, "add_job"));
                    return;
                }
                if(confId == null || confId.equals("")) {
                    model.put("message", "Missing required field confId");
                    ctx.render(Template.thymeleafTemplate(model, "add_job"));
                    return;
                }
                model.put("profile", profile);
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if(pkUser > -1) {
                        Blocking.get(() -> {
                            int pk;
                            DataSource plamadeDataSource = ctx.get(DataSource.class);
                            try (Connection connection = plamadeDataSource.getConnection()) {

                                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                                mapper.findAndRegisterModules();
                                DataBaseConfig dataBaseConfig = new DataBaseConfig();
                                SlurmConfigRoot slurmConfigList = mapper.readValue(ctx.file("config.yaml").toFile(), SlurmConfigRoot.class);
                                JsonNode cfg = mapper.readTree(ctx.file("config.yaml").toFile());
                                dataBaseConfig.user = cfg.findValue("database").findValue("user").asText();
                                dataBaseConfig.password = cfg.findValue("database").findValue("password").asText();
                                JsonNode apiTokenNode = cfg.findValue("auth").findValue("notificationAccessToken");
                                String apiToken = "";
                                if(apiTokenNode != null) {
                                    apiToken = apiTokenNode.asText();
                                }
                                long timeJob = System.currentTimeMillis();
                                String jobFolder = "dep" + inseeDepartment + "_" + timeJob;
                                String remoteJobFolder;
                                if(computeOnCluster) {
                                    remoteJobFolder = slurmConfigList.slurm.serverTempFolder + "/" + jobFolder;
                                } else {
                                    remoteJobFolder = NoiseModellingRunner.MAIN_JOBS_FOLDER + jobFolder +
                                            File.separator + NoiseModellingRunner.RESULT_DIRECTORY_NAME;
                                }
                                PreparedStatement statement = connection.prepareStatement(
                                        "INSERT INTO JOBS(REMOTE_JOB_FOLDER, LOCAL_JOB_FOLDER, CONF_ID, INSEE_DEPARTMENT, PK_USER, STATE)" +
                                                " VALUES (?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
                                statement.setString(1, remoteJobFolder);
                                statement.setString(2, jobFolder);
                                statement.setInt(3, Integer.parseInt(confId));
                                statement.setString(4, inseeDepartment);
                                statement.setInt(5, pkUser);
                                statement.setString(6, NoiseModellingRunner.JOB_STATES.QUEUED.toString());
                                statement.executeUpdate();
                                // retrieve primary key
                                ResultSet rs = statement.getGeneratedKeys();
                                if(rs.next()) {
                                    pk = rs.getInt(1);
                                    JobExecutorService pool = ctx.get(JobExecutorService.class);
                                    RootProgressVisitor rootProgressVisitor = new RootProgressVisitor(1, false, 5);
                                    rootProgressVisitor.addPropertyChangeListener("PROGRESS" , new ProgressionTracker(plamadeDataSource, pk));
                                    NoiseModellingRunner.Configuration configuration = new NoiseModellingRunner.Configuration(
                                            pkUser,new File(NoiseModellingRunner.MAIN_JOBS_FOLDER+
                                            File.separatorChar+jobFolder).getAbsolutePath(), Integer.parseInt(confId),
                                            inseeDepartment, pk, dataBaseConfig , rootProgressVisitor, remoteJobFolder);
                                    configuration.setNotificationAccessToken(apiToken);
                                    configuration.setComputeOnCluster(computeOnCluster);
                                    if(computeOnCluster) {
                                        configuration.setSlurmConfig(slurmConfigList.slurm);
                                    }
                                    pool.execute(new NoiseModellingRunner(
                                            configuration, plamadeDataSource));
                                } else {
                                    LOG.error("Could not insert new job without exceptions");
                                    return false;
                                }
                            }
                            return true;
                        }).then(ok -> {
                            model.put("message", ok ? "Job added" : "Could not create job");
                            ctx.render(Template.thymeleafTemplate(model, "add_job"));
                        });
                    }
                });
            }
        }));
    }

    private static class ProgressionTracker implements PropertyChangeListener {
        DataSource plamadeDataSource;
        int jobPk;
        private final Logger logger = LoggerFactory.getLogger(ProgressionTracker.class);
        String lastProg = "";
        long lastProgressionUpdate = 0;
        private static final long TABLE_UPDATE_DELAY = 5000;

        public ProgressionTracker(DataSource plamadeDataSource, int jobPk) {
            this.plamadeDataSource = plamadeDataSource;
            this.jobPk = jobPk;
        }

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            if(evt.getNewValue() instanceof Double) {
                String newLogProgress = String.format("%.2f", (Double)(evt.getNewValue()) * 100.0D);
                if(!lastProg.equals(newLogProgress)) {
                    lastProg = newLogProgress;
                    long t = System.currentTimeMillis();
                    if(t - lastProgressionUpdate > TABLE_UPDATE_DELAY) {
                        lastProgressionUpdate = t;
                        try (Connection connection = plamadeDataSource.getConnection()) {
                            PreparedStatement st = connection.prepareStatement("UPDATE JOBS SET PROGRESSION = ? WHERE PK_JOB = ?");
                            st.setDouble(1, (Double) (evt.getNewValue()) * 100.0);
                            st.setInt(2, jobPk);
                            st.setQueryTimeout((int)(TABLE_UPDATE_DELAY / 1000));
                            st.execute();
                        } catch (SQLException ex) {
                            logger.error(ex.getLocalizedMessage(), ex);
                        }
                    }
                }
            }
        }
    }
}
