package org.noise_planet.covadis.webserver.secure;

import io.javalin.security.RouteRole;

public enum Role implements RouteRole {ANYONE, RUNNER, ADMINISTRATOR}
