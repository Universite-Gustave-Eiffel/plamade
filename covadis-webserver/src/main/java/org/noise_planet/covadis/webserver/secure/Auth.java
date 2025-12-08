/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.secure;

import com.auth0.jwt.interfaces.DecodedJWT;
import io.javalin.http.Context;
import io.javalin.http.Header;
import io.javalin.http.HttpStatus;
import io.javalin.http.UnauthorizedResponse;
import org.noise_planet.covadis.webserver.utilities.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handle auth
 */
public class Auth {
    JWTProvider<User> provider;
    UserController userController;

    public Auth(JWTProvider<User> provider, UserController userController) {
        this.provider = provider;
        this.userController = userController;
    }

    /**
     * Check visitor credentials using Json Web Token.
     * Redirect user if non-authorized to the login page
     * @param ctx Javalin Web context
     */
    public void handleAccess(Context ctx) {
        var permittedRoles = ctx.routeRoles();
        if (permittedRoles.contains(Role.ANYONE)) {
            return; // anyone can access
        }
        if (userRoles(ctx).stream().anyMatch(permittedRoles::contains)) {
            return; // user has role required to access
        }
        ctx.redirect("login.html", HttpStatus.TEMPORARY_REDIRECT);
        throw new UnauthorizedResponse();
    }

    public List<Role> userRoles(Context context) {
        Optional<DecodedJWT> decodedJWT = JavalinJWT.getTokenFromHeader(context)
                .flatMap(provider::validateToken);
        if(decodedJWT.isPresent()) {

        } else {

        }
    }

}