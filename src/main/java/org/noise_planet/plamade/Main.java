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
package org.noise_planet.plamade;

import org.noise_planet.plamade.api.ApiEndpoints;
import org.noise_planet.plamade.api.ApiModule;
import org.noise_planet.plamade.auth.AuthModule;
import org.noise_planet.plamade.config.AuthConfig;
import org.pac4j.oidc.client.GoogleOidcClient;
import ratpack.groovy.template.TextTemplateModule;
import ratpack.guice.Guice;
import ratpack.pac4j.RatpackPac4j;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import ratpack.session.SessionModule;

import static ratpack.handling.Handlers.redirect;

/**
 * Starts the plamade application.
 */
public class Main {

    public static void main(String... args) throws Exception {
        RatpackServer.start(s -> s
                .serverConfig(c -> c
                        .yaml("config.yaml")
                        .env()
                        .require("/auth", AuthConfig.class)
                        .baseDir(BaseDir.find())
                        .build()
                )
                .registry(Guice.registry(b -> b
                        .module(ApiModule.class)
                        .module(AuthModule.class)
                        .module(TextTemplateModule.class)
                        .module(SessionModule.class))
                )
                .handlers(chain -> chain
                        .path(redirect(301, "index.html"))
                        .all(RatpackPac4j.authenticator(chain.getRegistry().get(GoogleOidcClient.class)))
                        .insert(ApiEndpoints.class)
                )
        );
    }
}
