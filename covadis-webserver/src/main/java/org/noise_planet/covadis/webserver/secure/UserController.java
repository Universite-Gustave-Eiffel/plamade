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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle user storage
 * Adapted from tutorial material from
 * <a href="https://github.com/javalin/javalin-samples/tree/main/javalin6/javalin-auth-example">javalin-auth-example</a>
 *
 */
public class UserController {

    private static final Map<Integer, User> users = new HashMap<>();
    private static final AtomicInteger userCount = new AtomicInteger(0);

    public static void addUser(String email, Role... roles) {
        User user = new User(userCount.getAndAdd(1), email, Arrays.asList(roles));
        users.put(user.identifier, user);
    }

    public void getAllUsers(Context ctx) {
        ctx.json(users.values());
    }

    public void createUser(Context ctx) {
        addUser(ctx.pathParam("email"));
    }

    public void getUser(Context ctx) {
        ctx.json(users.get(Integer.parseInt(ctx.pathParam("user_identifier"))));
    }

    public void updateUser(Context ctx) {
        // TODO
    }

    public void deleteUser(Context ctx) {
        users.remove(Integer.parseInt(ctx.pathParam("user_identifier")));
    }

}