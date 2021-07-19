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

/**
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.plamade.api.secure;

import groovy.util.Eval;
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
import java.io.File;
import java.sql.*;
import java.util.concurrent.ThreadPoolExecutor;

import static ratpack.jackson.Jackson.json;

/**
 * Create new job
 */
public class AddJob implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(AddJob.class);

    @Override
    public void handle(Context ctx) throws Exception {
        Promise<Form> form = ctx.parse(Form.class);
        form.then(f -> {
            final String inseeDepartment = f.get("INSEE_DEPARTMENT");
            final String confId = f.get("CONF_ID");
            if(inseeDepartment == null || inseeDepartment.equals("")) {
                ctx.render(json(Eval.me("[message: 'Missing required field inseeDepartment']")));
                return;
            }
            if(confId == null || confId.equals("")) {
                ctx.render(json(Eval.me("[message: 'Missing required field confId']")));
                return;
            }
            RatpackPac4j.userProfile(ctx).then(commonProfile -> {
                if (commonProfile.isPresent()) {
                    CommonProfile profile = commonProfile.get();
                    SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                        if(pkUser > -1) {
                            Blocking.get(() -> {
                                int pk;
                                Timestamp t = new Timestamp(System.currentTimeMillis());
                                try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                    PreparedStatement statement = connection.prepareStatement(
                                            "INSERT INTO JOBS(REMOTE_JOB_FOLDER, BEGIN_DATE, CONF_ID, INSEE_DEPARTMENT, PK_USER)" +
                                                    " VALUES (RANDOM_UUID(), ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
                                    statement.setObject(1, t);
                                    statement.setInt(2, Integer.parseInt(confId));
                                    statement.setString(3, inseeDepartment);
                                    statement.setInt(4, pkUser);
                                    statement.executeUpdate();
                                    // retrieve primary key
                                    ResultSet rs = statement.getGeneratedKeys();
                                    if(rs.next()) {
                                        pk = rs.getInt(1);
                                        ThreadPoolExecutor pool = ctx.get(ThreadPoolExecutor.class);
                                        pool.execute(new NoiseModellingInstance(
                                                new NoiseModellingInstance.Configuration(ctx.get(DataSource.class),
                                                        new File("jobs_running/"+pk).getAbsolutePath(),
                                                        Integer.parseInt(confId),
                                                        inseeDepartment
                                                )));
                                    } else {
                                        LOG.error("Could not insert new job without exceptions");
                                        return false;
                                    }
                                }
                                return true;
                            }).then(ok -> {
                                if(ok) {
                                    ctx.render(json(Eval.me("[message: 'Job added']")));
                                } else {
                                    ctx.render(json(Eval.me("[message: 'Could not create job']")));
                                }
                            });
                        }
                    });
                }
            });
        });
    }
}
