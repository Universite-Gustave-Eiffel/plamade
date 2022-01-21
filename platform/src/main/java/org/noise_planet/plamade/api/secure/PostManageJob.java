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
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.plamade.api.secure;

import com.google.common.collect.Maps;
import org.noise_planet.plamade.process.JobExecutorService;
import org.noise_planet.plamade.process.NoiseModellingInstance;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.form.Form;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

/**
 * Stop And/Or Delete jobs
 */
public class PostManageJob implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(PostManageJob.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> {
            RatpackPac4j.userProfile(ctx).then(commonProfile -> {
                if (commonProfile.isPresent()) {
                    CommonProfile profile = commonProfile.get();
                    final List<String> deleteJobsList = f.getAll("delete");
                    final List<String> cancelJobsList = f.getAll("cancel");
                    SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                        if(pkUser > -1) {
                            Blocking.get(() -> {
                                DataSource plamadeDataSource = ctx.get(DataSource.class);
                                JobExecutorService pool = ctx.get(JobExecutorService.class);
                                try (Connection connection = plamadeDataSource.getConnection()) {
                                    for(String jobId : cancelJobsList) {
                                        for(NoiseModellingInstance instance : pool.getNoiseModellingInstance()) {
                                            if(instance.getConfiguration().getTaskPrimaryKey() == Integer.parseInt(jobId)) {
                                                instance.cancel(false);
                                                break;
                                            }
                                        }
                                    }
                                    for(String jobId : deleteJobsList) {
                                        PreparedStatement st = connection.prepareStatement("DELETE FROM JOBS WHERE pk_job = ? AND pk_user = ?");
                                        st.setInt(1, Integer.parseInt(jobId));
                                        st.setInt(2, pkUser);
                                        st.execute();
                                    }
                                }
                                return true;
                            }).then(ok -> {
                                ctx.redirect("/manage/job_list");
                            });
                        }
                    });
                }
            });
        });
    }
}
