/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.secure;

import com.atlassian.onetime.core.TOTPGenerator;
import com.atlassian.onetime.model.EmailAddress;
import com.atlassian.onetime.model.Issuer;
import com.atlassian.onetime.model.TOTPSecret;
import com.atlassian.onetime.service.*;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.javalin.http.Context;
import io.javalin.http.InternalServerErrorResponse;
import io.javalin.http.util.NaiveRateLimit;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.Connection;
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
    private final TOTPService totpService;
    private final TOTPGenerator totpGenerator;
    private final TOTPConfiguration totpConfiguration;


    public UserController(DataSource serverDataSource, JWTProvider<User> provider) {
        this.serverDataSource = serverDataSource;
        this.provider = provider;
        totpGenerator = new TOTPGenerator();
        totpConfiguration = new TOTPConfiguration();
        totpService = new DefaultTOTPService(
                totpGenerator,
                totpConfiguration
        );
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
        String token = ctx.pathParam("token");
        TOTPSecret totpSecret = RandomSecretProvider.Companion.generateSecret();
        try(Connection connection = serverDataSource.getConnection()) {
            int userIdentifier = DatabaseManagement.getUserByRegisterToken(connection, token);
            if(userIdentifier >= 0) {
                User user = DatabaseManagement.getUser(connection, userIdentifier);
                URI totpUri = totpService.generateTOTPUrl(
                        totpSecret,
                        new EmailAddress(user.email),
                        new Issuer("NoiseModelling"));

                ctx.render("register.html", Map.of("token", token, "totpUri", totpUri));
            }  else {
                ctx.render("register.html", Map.of("messages", List.of("Register/Reset token is no longer valid"), "token", token));
            }
        } catch (SQLException e) {
            throw new InternalServerErrorResponse();
        }
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