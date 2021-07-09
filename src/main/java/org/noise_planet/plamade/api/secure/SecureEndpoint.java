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
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.google.common.collect.Maps;
import org.noise_planet.plamade.config.AdminConfig;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import static ratpack.jackson.Jackson.json;
import ratpack.pac4j.RatpackPac4j;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static ratpack.groovy.Groovy.groovyTemplate;

public class SecureEndpoint implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(SecureEndpoint.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx)
                .then(commonProfile -> {
                    final Map<String, Object> model = Maps.newHashMap();
                    if (commonProfile.isPresent()) {
                        CommonProfile profile = commonProfile.get();
                        model.put("profile", profile);

                        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                        AdminConfig adminConfig = mapper.readValue(ctx.file("config.yaml").toFile(), AdminConfig.class);

                        model.put("admins", adminConfig);
                        ctx.render(groovyTemplate(model, "secure.html"));
                    } else {
                        ctx.redirect("index.html");
                    }
                });
    }

    public static Promise<Integer> getUserPk(Context ctx, CommonProfile profile) {
        return Blocking.get(() -> {
            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM USERS" + " WHERE " +
                        "USER_OID = ?");
                statement.setString(1, profile.getId());
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        // found user
                        return rs.getInt("PK_USER");
                    } else {
                        // Nope
                        return -1;
                    }
                }
            }
        });
    }
}
