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
import org.noise_planet.plamade.process.NoiseModellingRunner;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import java.util.List;
import java.util.Map;

public class GetJobLogs implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(GetJobLogs.class);
    static final int FETCH_NUMBER_OF_LINES = 1000;

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser != -1) {
                        final Map<String, Object> model = Maps.newHashMap();
                        final String jobId = ctx.getAllPathTokens().get("jobid");
                        List<String> rows = NoiseModellingRunner.getAllLines(jobId, FETCH_NUMBER_OF_LINES);
                        LOG.info(String.format("Got %d log rows", rows.size()));
                        model.put("rows", rows);
                        model.put("profile", profile);
                        ctx.render(Template.thymeleafTemplate(model, "joblogs"));
                    }
                });
            } else {
                ctx.render(Template.thymeleafTemplate("blank"));
            }
        });
    }
}
