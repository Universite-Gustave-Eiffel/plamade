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
import io.javalin.http.UnauthorizedResponse;
import org.noise_planet.covadis.webserver.UserController;

import java.sql.SQLException;

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
        // Read visitor token
        int userIdentifier = JavalinJWT.getUserIdentifierFromContext(ctx, provider);
        if(userIdentifier >= 0) {
            try {
                User user = userController.getUser(userIdentifier);
                if(!user.registerToken.isEmpty()) {
                    // The administrator has reset the TOTP code
                    // User must validate the new TOTP code to be able to log in
                    ctx.attribute("messages",  "Unauthorized access please login before proceeding");
                    throw new UnauthorizedResponse();
                }
                if (user.roles.stream().noneMatch(permittedRoles::contains)) {
                    ctx.attribute("messages", "You do not have the minimal authorization access to see this page");
                    throw new UnauthorizedResponse();
                }
                ctx.attribute("user", user);
            } catch (SQLException e) {
                ctx.attribute("messages", "Exception while authenticating the user");
                throw new UnauthorizedResponse();
            }
        } else {
            ctx.attribute("messages",  "Unauthorized access please login before proceeding");
            throw new UnauthorizedResponse();
        }
    }
}