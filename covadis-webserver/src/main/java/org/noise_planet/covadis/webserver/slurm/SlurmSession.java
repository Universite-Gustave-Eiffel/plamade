/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.slurm;

import com.google.common.io.CountingInputStream;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.session.SessionContext;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sshd.common.util.io.IoUtils;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.session.SessionContext;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.common.util.io.IoUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collections;
import java.security.GeneralSecurityException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;



public class SlurmSession {
    private static final Logger logger = LoggerFactory.getLogger(SlurmSession.class);

    public static final SlurmJobKnownStatus[] SLURM_JOB_KNOWN_STATUSES = new SlurmJobKnownStatus[]{
            new SlurmJobKnownStatus("COMPLETED", true, false), // The job has completed successfully.
            new SlurmJobKnownStatus("COMPLETING", false, false), // The job is finishing but some processes are still active.
            new SlurmJobKnownStatus("FAILED", true, true), // The job terminated with a non-zero exit code and failed to execute.
            new SlurmJobKnownStatus("PENDING", false, false), // The job is waiting for resource allocation. It will eventually run.
            new SlurmJobKnownStatus("PREEMPTED", false, false), // The job was terminated because of preemption by another job.
            new SlurmJobKnownStatus("RUNNING", false, false), // The job currently is allocated to a node and is running.
            new SlurmJobKnownStatus("SUSPENDED", false, false), // A running job has been stopped with its cores released to other jobs.
            new SlurmJobKnownStatus("STOPPED", true, false), // A running job has been stopped with its cores retained.
            new SlurmJobKnownStatus("CANCELED", true, true), // Job canceled by system or user
            new SlurmJobKnownStatus("TIMEOUT", true, true) // Job timeout (will not be restarted)
    };

    private static final int SFTP_TIMEOUT = 60000;
    private static final int POLL_SLURM_STATUS_TIME = 40000;

    private static final String BATCH_FILE_NAME = "noisemodelling_batch.sh";
    private int oldFinishedJobs = 0;
    private Map<String, SlurmJobKnownStatus> slurmStateMap = new TreeMap<>();

    public SlurmSession() {
        // Loop check for job status
        for(SlurmJobKnownStatus s : SLURM_JOB_KNOWN_STATUSES) {
            slurmStateMap.put(s.status, s);
        }
    }

    public static ClientSession openSshSession(SlurmConfig slurmConfig) throws IOException, GeneralSecurityException {
        // Opens authenticated SSH session to remote host

        // 1. Prepare the input stream
        InputStream keyStream = new ByteArrayInputStream(slurmConfig.sshKeyArmoredString.getBytes(StandardCharsets.UTF_8));

        // 3. Define the Password Provider
        // The provider receives (SessionContext, NamedResource, int retryIndex)
        FilePasswordProvider passwordProvider = (session, resource, retryIndex) -> slurmConfig.sshKeyPassword;
        // 2. Load the KeyPair
        Iterable<KeyPair> keyPairs = SecurityUtils.loadKeyPairIdentities(
                null,
                () -> "in-memory-key",
                keyStream,
                slurmConfig.sshKeyPassword.isEmpty() ? null : passwordProvider
        );
        try(SshClient client = SshClient.setUpDefaultClient()) {
            // Set the KeyIdentityProvider with the loaded keys
            client.setKeyIdentityProvider(KeyIdentityProvider.wrapKeyPairs(keyPairs));

            client.start();
            // Configure known hosts
            client.setServerKeyVerifier((clientSession, remoteAddress, serverKey) -> {
                String serverKeyAlgorithm = serverKey.getAlgorithm();
                String encodedKey = Base64.getEncoder().encodeToString(serverKey.getEncoded());

                if (serverKeyAlgorithm.equals(slurmConfig.serverKeyType) && encodedKey.equals(slurmConfig.serverKey)) {
                    return true;
                }

                logger.error(
                        "Unknown host. Use the following configuration in the slurm configuration if you trust this server:\n" + " serverKeyType:\"{}\"\n serverKey:\"{}\"",
                        serverKeyAlgorithm, encodedKey);
                return false;
            });

            ClientSession session = client.connect(slurmConfig.user, slurmConfig.host, slurmConfig.port)
                    .verify(SFTP_TIMEOUT).getSession();

            session.auth().verify(SFTP_TIMEOUT);
            logger.info("Successfully connected to the server {}", slurmConfig.host);

            return session;
        }
    }

    /**
     * Executes a command on a remote server using an SSH client session and captures the output.
     * The output lines are optionally logged and returned as a list of strings.
     * Additionally, it tracks the number of bytes read from the remote execution and updates the provided {@code AtomicLong}.
     *
     * @param session The active SSH client session to use for executing the command.
     * @param command The command string to be executed on the remote server.
     * @param logResult Indicates whether the output of the command should be logged.
     * @param readBytes An {@code AtomicLong} instance that will be updated to reflect the number of bytes read during execution.
     * @return A list of strings, where each string represents a line of output from the executed command.
     * @throws IOException If an error occurs during command execution or communication over the SSH channel.
     */
    public List<String> runCommand(ClientSession session, String command, boolean logResult, AtomicLong readBytes)
            throws IOException {
        List<String> lines = new ArrayList<>();
        try (ChannelExec shell = session.createExecChannel(command)) {
            shell.setRedirectErrorStream(false);
            shell.setErr(System.err);
            shell.open().verify(SFTP_TIMEOUT);

            InputStream in = shell.getInvertedOut();

            CountingInputStream countingInputStream = new CountingInputStream(in);
            InputStreamReader inputStreamReader = new InputStreamReader(countingInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            while (true) {
                String line = bufferedReader.readLine();
                if (line != null) {
                    if (logResult) {
                        logger.info(line);
                    }
                    lines.add(line);
                } else {
                    if (shell.isClosed()) {
                        Integer exitStatus = shell.getExitStatus();
                        if (exitStatus != null && exitStatus != 0) {
                            logger.error(String.format("Command %s \n exit-status: %d", command, exitStatus));
                        }
                    } else {
                        logger.warn("Stream is closed but the channel is still open");
                    }
                    break;
                }
            }
            readBytes.addAndGet(countingInputStream.getCount());
        }

        return lines;
    }
}
