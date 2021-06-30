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

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;

import java.util.Map;

import static ratpack.groovy.Groovy.groovyTemplate;
import static ratpack.jackson.Jackson.json;

public class JobList implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(JobList.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx)
                .then(commonProfile -> {
                    final Map<String, Object> model = Maps.newHashMap();
                    model.put("profile", commonProfile);

                    //ctx.render(groovyTemplate(model, "secure.html"));
                    if(commonProfile.isPresent()) {
                        ctx.render(json(commonProfile.get()));
                    } else {
                        ctx.render(groovyTemplate(model, "nonsecure.html"));
                    }
                });
    }
}
