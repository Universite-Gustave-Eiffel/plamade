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
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ratpack.jackson.Jackson.json;


/**
 * Authenticated user is not in job manager list. This Rest Api will let the user subscribe to the
 */
public class Subscribe implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(Subscribe.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser == -1) {
                        Blocking.get(() -> {
                            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                PreparedStatement statement = connection.prepareStatement(
                                        "MERGE INTO USER_ASK_INVITATION(USER_OID, MAIL) KEY(USER_OID) VALUES (?,?)");
                                statement.setString(1, profile.getId());
                                statement.setString(2, profile.getEmail());
                                statement.execute();
                            }
                            return true;
                        }).then(ok -> {
                            ctx.render(json(Eval.me("[message: 'Please wait for account approval..']")));
                        });
                    }
                });
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
