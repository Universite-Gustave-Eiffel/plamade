package org.noise_planet.covadis.webserver.utilities;

import java.util.Objects;

public class Pair {
    private final String a;
    private final String b;

    public Pair(String a, String b) {
        this.a = a;
        this.b = b;
    }

    public String a() {
        return a;
    }

    public String b() {
        return b;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Pair) obj;
        return Objects.equals(this.a, that.a) &&
                Objects.equals(this.b, that.b);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }

    @Override
    public String toString() {
        return "Pair[" +
                "a=" + a + ", " +
                "b=" + b + ']';
    }
}