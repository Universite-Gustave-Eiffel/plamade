/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.secure;

import io.javalin.http.Context;
import io.javalin.http.Header;
import io.javalin.http.UnauthorizedResponse;
import org.noise_planet.covadis.webserver.utilities.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handle auth
 */
public class Auth {

    public static void handleAccess(Context ctx) {
        var permittedRoles = ctx.routeRoles();
        if (permittedRoles.contains(Role.ANYONE)) {
            return; // anyone can access
        }
        if (userRoles(ctx).stream().anyMatch(permittedRoles::contains)) {
            return; // user has role required to access
        }
        ctx.header(Header.WWW_AUTHENTICATE, "Basic");
        throw new UnauthorizedResponse();
    }

    public static List<Role> userRoles(Context ctx) {
        return Optional.ofNullable(ctx.basicAuthCredentials())
                .map(credentials -> userRolesMap.getOrDefault(new Pair(credentials.getUsername(), credentials.getPassword()), List.of()))
                .orElse(List.of());
    }

    private static final Map<Pair, List<Role>> userRolesMap = Map.of(
            new Pair("alice", "weak-1234"), List.of(Role.RUNNER),
            new Pair("bob", "weak-123456"), List.of(Role.RUNNER, Role.ADMINISTRATOR)
    );

}