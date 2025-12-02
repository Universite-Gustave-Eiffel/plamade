package org.noise_planet.plamade.webserver.secure;

import io.javalin.security.RouteRole;

public enum Role implements RouteRole {ANYONE, USER_READ, USER_WRITE}
