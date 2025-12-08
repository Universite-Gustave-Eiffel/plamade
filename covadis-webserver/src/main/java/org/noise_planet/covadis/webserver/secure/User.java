/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.secure;

import java.util.List;

public class User {

    public User(int identifier, String email, List<Role> roles) {
        this.identifier = identifier;
        this.email = email;
        this.roles = roles;
    }

    /** Database identifier for this user */
    int identifier;
    /** User email or name */
    String email;
    /** User roles/rights */
    List<Role> roles;
    /** User TOTP secret token */
    String secretToken;

}

