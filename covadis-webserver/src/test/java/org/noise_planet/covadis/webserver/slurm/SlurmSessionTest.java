package org.noise_planet.covadis.webserver.slurm;

import org.apache.sshd.client.session.ClientSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class SlurmSessionTest {
    private static final Logger logger = LoggerFactory.getLogger(SlurmSessionTest.class);

    @Test
    public void testOpenSshSession() throws Exception {
        String host = System.getenv("SSH_HOST");
        String portStr = System.getenv("SSH_PORT");
        String user = System.getenv("SSH_USER");
        String key = System.getenv("SSH_KEY");
        String keyPassword = System.getenv("SSH_KEY_PASSWORD");
        String serverKey = System.getenv("SSH_SERVER_KEY");
        String serverKeyType = System.getenv("SSH_SERVER_KEY_TYPE");

        Assumptions.assumeTrue(host != null && !host.isEmpty(), "SSH_HOST is not set");
        Assumptions.assumeTrue(user != null && !user.isEmpty(), "SSH_USER is not set");
        Assumptions.assumeTrue(key != null && !key.isEmpty(), "SSH_KEY is not set");

        SlurmConfig config = new SlurmConfig();
        config.host = host;
        config.port = portStr != null && !portStr.isEmpty() ? Integer.parseInt(portStr) : 22;
        config.user = user;
        config.sshKeyArmoredString = key;
        config.sshKeyPassword = keyPassword != null ? keyPassword : "";
        config.serverKey = serverKey;
        config.serverKeyType = serverKeyType;

        logger.info("Connecting to {}:{} as {}", host, config.port, user);
        try (SlurmSession slurmSession = new SlurmSession(config)) {
            slurmSession.connect();
            assertNotNull(slurmSession.getSession());
            assertTrue(slurmSession.getSession().isAuthenticated());

            AtomicLong readBytes = new AtomicLong(0);
            List<String> output = slurmSession.runCommand("whoami", true, readBytes);
            assertFalse(output.isEmpty(), "Command output should not be empty");
            String actualUser = output.get(0).trim();
            // In some environments (like linuxserver/openssh-server), whoami might return the PUID's user or the USER_NAME.
            // For linuxserver/openssh-server with USER_NAME=testuser, it should be testuser.
            assertTrue(actualUser.equals(user) || actualUser.contains(user), "User mismatch. Expected " + user + " but got " + actualUser);
            assertTrue(readBytes.get() > 0, "Read bytes should be greater than zero");

            // Test unlimited timeout (0)
            readBytes.set(0);
            output = slurmSession.runCommand("echo 'unlimited'", true, readBytes, 0);
            assertFalse(output.isEmpty(), "Command output should not be empty");
            assertEquals("unlimited", output.get(0).trim());
        } catch (Exception e) {
            logger.error("SSH connection failed", e);
            throw e;
        }
    }
}
