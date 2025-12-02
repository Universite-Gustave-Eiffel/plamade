package org.noise_planet.plamade.webserver.secure;

import io.javalin.http.Context;
import java.util.*;

/**
 * Handle user storage
 * Adapted from tutorial material from
 * https://github.com/javalin/javalin-samples/tree/main/javalin6/javalin-auth-example
 *
 */
public class UserController {
    public static final class User {
        String name;
        String email;
        public User(String name, String email) {
            this.name = name;
            this.email = email;
        }
    }

    private static final Map<String, User> users;

    static {
        var tempMap = Map.of(
                randomId(), new User("Alice", "alice@alice.kt"),
                randomId(), new User("Bob", "bob@bob.kt"),
                randomId(), new User("Carol", "carol@carol.kt"),
                randomId(), new User("Dave", "dave@dave.kt")
        );
        users = new HashMap<>(tempMap);
    }

    public static void getAllUserIds(Context ctx) {
        ctx.json(users.keySet());
    }

    public static void createUser(Context ctx) {
        users.put(randomId(), ctx.bodyAsClass(User.class));
    }

    public static void getUser(Context ctx) {
        ctx.json(users.get(ctx.pathParam("userId")));
    }

    public static void updateUser(Context ctx) {
        users.put(ctx.pathParam("userId"), ctx.bodyAsClass(User.class));
    }

    public static void deleteUser(Context ctx) {
        users.remove(ctx.pathParam("userId"));
    }

    private static String randomId() {
        return UUID.randomUUID().toString();
    }
}