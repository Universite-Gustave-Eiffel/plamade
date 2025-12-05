package org.noise_planet.covadis.webserver.utilities;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Hash {


    /**
     * Generate a short hash from a string.
     * The hash is URL-safe Base64 encoded, without padding.
     *
     * @param string The input string to hash.
     * @return A short URL-safe Base64 encoded hash string.
     */
    public static String shortHash(String string) {
        return shortHash(string, 8);
    }

    /**
     * Generate a short hash from a file.
     * The hash is URL-safe Base64 encoded, without padding.
     *
     * @param string The input string to hash.
     * @param maxLength The number of bytes to use from the hash (default is 8).
     * @return A short URL-safe Base64 encoded hash string.
     */
    public static String shortHash(String string, int maxLength) {
        try {
            // You can replace "SHA-256" with any digest; we only use the first n bytes.
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] fullHash = digest.digest(string.getBytes(StandardCharsets.UTF_8));

            // Take first n bytes (32 bits) for a short hash
            byte[] shortBytes = new byte[maxLength];
            System.arraycopy(fullHash, 0, shortBytes, 0, Math.min(shortBytes.length, fullHash.length));

            // URL-safe Base64, no padding, only A-Z a-z 0-9 - _
            return Base64.getUrlEncoder().withoutPadding().encodeToString(shortBytes);
        } catch (NoSuchAlgorithmException e) {
            // Should not happen for SHA-256 on a normal JVM
            throw new RuntimeException("Hash algorithm not available", e);
        }
    }
}
