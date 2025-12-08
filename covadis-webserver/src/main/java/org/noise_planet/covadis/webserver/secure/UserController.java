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
import io.javalin.http.InternalServerErrorResponse;
import io.javalin.http.util.NaiveRateLimit;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Handle users management
 * Adapted from tutorial material from
 * <a href="https://github.com/javalin/javalin-samples/tree/main/javalin6/javalin-auth-example">javalin-auth-example</a>
 * Do not add SQL queries in this class
 */
public class UserController {
    private final DataSource serverDataSource;
    private final JWTProvider<User> provider;

    public UserController(DataSource serverDataSource, JWTProvider<User> provider) {
        this.serverDataSource = serverDataSource;
        this.provider = provider;
    }

    User getUser(int userIdentifier) throws SQLException {
        return DatabaseManagement.getUser(serverDataSource, userIdentifier);
    }

    public void login(Context ctx ) {
        ctx.render("login.html", Map.of("messages", ctx.queryParams("messages")));
    }

    public void doLogin(Context ctx ) {
        // brute force protection
        NaiveRateLimit.requestPerTimeUnit(ctx, 5, TimeUnit.MINUTES);
    }


    public void register(Context ctx ) {
        ctx.render("register.html", Map.of("messages", ctx.queryParams("messages")));
    }

    public void getAllUsers(Context ctx) {

    }

    public void createUser(Context ctx) {
    }


    public void updateUser(Context ctx) {
        // TODO
    }

    public void deleteUser(Context ctx) {


    }

}