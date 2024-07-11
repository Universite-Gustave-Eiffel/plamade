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
import org.noise_planet.plamade.process.GeoClimateRunner;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/*
TODO :
    - ADD parameters in file (like JSON) for don't give a lot of parameters of construction constructor.
 */

/**
 * @Author Samuel Marsault, Trainee
 */

/**
 * Create new job
 */
public class PostUseGeoclimate implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(PostUseGeoclimate.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                final Map<String, Object> model = Maps.newHashMap();
                final String location = f.get("LOCATION");
                final String outputPath = Paths.get(f.get("OUTPUT_RESULT")).isAbsolute() ? f.get("OUTPUT_RESULT") : GeoClimateRunner.MAIN_JOBS_FOLDER;
                final String srid = f.get("SRID");
                final String confId = f.get("CONF_ID");
                final String cellDist = f.get("MAX_CELL_DIST");
                final String height = f.get("HEIGHT");
                final String roadWidth = f.get("ROAD_WIDTH");
                final String maxArea = f.get("MAX_AREA");
                final String wall_alpha = f.get("WALL_ALPHA");
                final String reflexion_order = f.get("REFLEXION");
                final String max_src_dist = f.get("SRC_DIST");
                final String max_ref_dist = f.get("SRC_REFL");
                final String n_thread = f.get("THREAD");
                final String confHumidity = f.get("HUMIDITY");
                final String confTemperature = f.get("TEMPERATURE");
                final String confFavorableOccurrencesDay = f.get("PB_DAY");
                final String confFavorableOccurrencesEvening = f.get("PB_EVENING");
                final String confFavorableOccurrencesNight = f.get("PB_NIGHT");
                final boolean saveDatabase = Boolean.parseBoolean(f.getOrDefault(
                        "TEMP_BASE", Boolean.FALSE.toString()));
                final boolean computeOnCluster = Boolean.parseBoolean(f.getOrDefault(
                        "CLUSTER_COMPUTE", Boolean.FALSE.toString()));
                final boolean isoBuilding = Boolean.parseBoolean(f.getOrDefault(
                        "ISO_BUILDING", Boolean.FALSE.toString()));
                final boolean compute_horizontal_diffraction = Boolean.parseBoolean(f.getOrDefault(
                        "DIF_HORI", Boolean.FALSE.toString()));
                final boolean compute_vertical_diffraction = Boolean.parseBoolean(f.getOrDefault(
                        "DIF_VERT", Boolean.FALSE.toString()));
                final boolean confSkipLday = Boolean.parseBoolean(f.getOrDefault(
                        "LDAY", Boolean.FALSE.toString()));
                final boolean confSkipLevening = Boolean.parseBoolean(f.getOrDefault(
                        "LEVENING", Boolean.FALSE.toString()));
                final boolean confSkipLnight = Boolean.parseBoolean(f.getOrDefault(
                        "LNIGHT", Boolean.FALSE.toString()));
                final boolean confSkipLden = Boolean.parseBoolean(f.getOrDefault(
                        "LDEN", Boolean.FALSE.toString()));
                final boolean confExportSourceId = Boolean.parseBoolean(f.getOrDefault(
                        "SOURCE_ID", Boolean.FALSE.toString()));

                if(location == null || location.equals("")) {
                    model.put("message", "Missing required field location");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(srid == null || srid.equals("")) {
                    model.put("message", "Missing required field srid");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(outputPath == null || outputPath.equals("")) {
                    model.put("message", "Missing required field output Path");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confId == null || confId.equals("")) {
                    model.put("message", "Missing required field confId");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(cellDist == null || cellDist.equals("")) {
                    model.put("message", "Missing required field cellDist");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(height == null || height.equals("")) {
                    model.put("message", "Missing required field height");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(roadWidth == null || roadWidth.equals("")) {
                    model.put("message", "Missing required field road width");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(maxArea == null || maxArea.equals("")) {
                    model.put("message", "Missing required field max area");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(wall_alpha == null || wall_alpha.equals("")) {
                    model.put("message", "Missing required field wall alpha");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(reflexion_order == null ||reflexion_order.equals("")) {
                    model.put("message", "Missing required field reflexion order");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(max_src_dist == null || max_src_dist.equals("")) {
                    model.put("message", "Missing required field max scr distance");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(max_ref_dist == null || max_ref_dist.equals("")) {
                    model.put("message", "Missing required field max reflexion distance");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(n_thread == null || n_thread.equals("")) {
                    model.put("message", "Missing required field number of thread");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confHumidity == null || confHumidity.equals("")) {
                    model.put("message", "Missing required field humidity");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confTemperature == null || confTemperature.equals("")) {
                    model.put("message", "Missing required field temperature");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confFavorableOccurrencesDay == null || confFavorableOccurrencesDay.equals("")) {
                    model.put("message", "Missing required field favorable occurence day");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confFavorableOccurrencesEvening == null || confFavorableOccurrencesEvening.equals("")) {
                    model.put("message", "Missing required field favorable occurence evening");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                    return;
                }
                if(confFavorableOccurrencesNight == null || confFavorableOccurrencesNight.equals("")) {
                    model.put("message", "Missing required field favorable occurence night");
                    ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
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
                                String jobFolder = "location_"+ location + "_" + timeJob;
                                String remoteJobFolder;
                                if(computeOnCluster) {
                                    remoteJobFolder = slurmConfigList.slurm.serverTempFolder + "/" + jobFolder;
                                } else {
                                    remoteJobFolder = new File(GeoClimateRunner.MAIN_JOBS_FOLDER + File.separatorChar + jobFolder).getAbsolutePath().toString();
                                }
                                PreparedStatement statement = connection.prepareStatement(
                                        "INSERT INTO JOBS(REMOTE_JOB_FOLDER, LOCAL_JOB_FOLDER, CONF_ID, INSEE_DEPARTMENT, PK_USER, STATE)" +
                                                " VALUES (?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
                                statement.setString(1, remoteJobFolder);
                                statement.setString(2, jobFolder);
                                statement.setInt(3, Integer.parseInt(confId));
                                statement.setString(4, location);
                                statement.setInt(5, pkUser);
                                statement.setString(6, GeoClimateRunner.JOB_STATES.QUEUED.toString());
                                statement.executeUpdate();
                                // retrieve primary key
                                ResultSet rs = statement.getGeneratedKeys();
                                if(rs.next()) {
                                    pk = rs.getInt(1);
                                    JobExecutorService pool = ctx.get(JobExecutorService.class);
                                    RootProgressVisitor rootProgressVisitor = new RootProgressVisitor(1, false, 5);
                                    rootProgressVisitor.addPropertyChangeListener("PROGRESS" , new ProgressionTracker(plamadeDataSource, pk));
                                    GeoClimateRunner.Configuration configuration = new GeoClimateRunner.Configuration(
                                            pkUser,
                                            new File( outputPath + File.separator + jobFolder +
                                                    File.separator + GeoClimateRunner.RESULT_DIRECTORY_NAME).getAbsolutePath(),
                                            Integer.parseInt(confId),
                                            location,
                                            pk,
                                            dataBaseConfig,
                                            rootProgressVisitor,
                                            remoteJobFolder,
                                            location,
                                            srid,
                                            saveDatabase,
                                            isoBuilding,
                                            cellDist,
                                            height,
                                            roadWidth,
                                            maxArea,
                                            wall_alpha,
                                            reflexion_order,
                                            max_src_dist,
                                            max_ref_dist,
                                            n_thread,
                                            confHumidity,
                                            confTemperature,
                                            confFavorableOccurrencesDay,
                                            confFavorableOccurrencesEvening,
                                            confFavorableOccurrencesNight,
                                            computeOnCluster,
                                            compute_horizontal_diffraction,
                                            compute_vertical_diffraction,
                                            confSkipLday,
                                            confSkipLevening,
                                            confSkipLnight,
                                            confSkipLden,
                                            confExportSourceId
                                    );

                                    configuration.setNotificationAccessToken(apiToken);
                                    configuration.setComputeOnCluster(computeOnCluster);
                                    if(computeOnCluster) {
                                        configuration.setSlurmConfig(slurmConfigList.slurm);
                                    }
                                    pool.execute(new GeoClimateRunner(
                                            configuration, plamadeDataSource));
                                } else {
                                    LOG.error("Could not insert new job without exceptions");
                                    return false;
                                }
                            }
                            return true;
                        }).then(ok -> {
                            model.put("message", ok ? "Job added" : "Could not create job");
                            ctx.render(Template.thymeleafTemplate(model, "use_geoclimate"));
                        });
                    }
                });
            }
        }));
    }

    private static class ProgressionTracker implements PropertyChangeListener {
        DataSource plamadeDataSource;
        int jobPk;
        private final Logger logger = LoggerFactory.getLogger(PostUseGeoclimate.ProgressionTracker.class);
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
