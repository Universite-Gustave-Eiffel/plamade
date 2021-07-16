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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import groovy.util.Eval;
import org.noise_planet.plamade.config.AdminConfig;
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
import java.util.Collections;

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
            RatpackPac4j.userProfile(ctx).then(commonProfile -> {
                if (commonProfile.isPresent()) {
                    CommonProfile profile = commonProfile.get();
                    SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                        if(pkUser > -1) {
                            Blocking.get(() -> {
                                try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                    PreparedStatement statement = connection.prepareStatement(
                                            "INSERT INTO JOBS(BEGIN_DATE, CONF_ID, INSEE_DEPARTMENT, PK_USER)" +
                                                    " VALUES (NOW(), ?, ?, ?)");
                                    statement.setInt(1, Integer.parseInt(confId));
                                    statement.setString(2, inseeDepartment);
                                    statement.setInt(3, pkUser);
                                    statement.execute();
                                }
                                return true;
                            }).then(ok -> {
                                ctx.render(json(Eval.me("[message: 'Job added']")));
                            });
                        }
                    });
                }
            });
        });
    }
}
