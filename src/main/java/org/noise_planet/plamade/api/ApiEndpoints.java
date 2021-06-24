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
package org.noise_planet.plamade.api;

import org.noise_planet.plamade.api.secure.SecureEndpoint;
import org.pac4j.oidc.client.GoogleOidcClient;
import ratpack.func.Action;
import ratpack.handling.Chain;
import ratpack.pac4j.RatpackPac4j;

import static ratpack.groovy.Groovy.groovyTemplate;

public class ApiEndpoints implements Action<Chain> {

    @Override
    public void execute(Chain chain) throws Exception {
        // Endpoint that requires the user to be logged in
        chain.prefix("manage", c -> {
            c.all(RatpackPac4j.requireAuth(GoogleOidcClient.class));
            c.get(SecureEndpoint.class);
        });

        // Logout
        chain.path("logout", ctx ->
                RatpackPac4j.logout(ctx).then(() -> ctx.redirect("index.html"))
        );

        // Main Page
        chain.get("index.html", ctx -> {
            ctx.render(groovyTemplate("index.html"));
        });
    }
}
