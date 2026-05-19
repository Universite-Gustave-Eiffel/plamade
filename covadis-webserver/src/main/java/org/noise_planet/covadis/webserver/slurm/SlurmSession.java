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
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.h2gis.api.ProgressVisitor;
import org.noise_planet.covadis.webserver.utilities.LoggingOutputStream;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents a session for interacting with a SLURM (Simple Linux Utility for Resource Management) workload manager
 * over SSH. The class provides utilities for managing SLURM job states, executing commands on remote servers,
 * and maintaining an authenticated SSH session.
 */
public class SlurmSession implements AutoCloseable {

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

    public static final int SFTP_TIMEOUT = 60000;
    public static final int POLL_SLURM_STATUS_TIME = 40000;

    public static final String DEFAULT_BATCH_FILE_NAME = "noisemodelling_batch.sh";
    protected Map<String, SlurmJobKnownStatus> slurmStateMap = new TreeMap<>();
    protected final SlurmConfig slurmConfig;
    protected SshClient client;
    protected ClientSession session;
    private final Logger logger;

    public SlurmSession(SlurmConfig slurmConfig, Logger logger) {
        this.slurmConfig = slurmConfig;
        this.logger = logger;
        // Loop check for job status
        for(SlurmJobKnownStatus s : SLURM_JOB_KNOWN_STATUSES) {
            slurmStateMap.put(s.status, s);
        }
        if (!SecurityUtils.isBouncyCastleRegistered()) {
            throw new IllegalStateException("BouncyCastle is required for Ed25519 support but not registered!");
        }
    }

    /**
     * Reformats an SSH key string that may have been altered by an editor,
     * which can introduce unwanted spaces and remove newlines.
     * The method attempts to restore the original format of the SSH key by identifying the BEGIN and END markers
     * and reconstructing the key with proper newlines.
     * @param rawKey The raw SSH key string that may have been modified by an editor.
     * @return The reformatted SSH key string with proper newlines, or the original string if it cannot be reformatted.
     */
    protected static String reformatSSHKey(String rawKey) {
        if (rawKey == null || rawKey.isEmpty()) {
            return rawKey;
        }

        // 1. If it already has newlines, it's likely already correct
        if (rawKey.contains("\n") || rawKey.contains("\r")) {
            return rawKey;
        }

        // 2. Regex to identify the BEGIN marker, the middle content, and the END marker
        // Supports: OPENSSH, RSA, DSA, EC, and PRIVATE KEY
        String regex = "(-----BEGIN [A-Z ]+-----)(.*)(-----END [A-Z ]+-----)";

        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
        java.util.regex.Matcher matcher = pattern.matcher(rawKey.trim());

        if (matcher.find()) {
            String header = matcher.group(1);
            String content = matcher.group(2).replace(" ", ""); // Remove any spaces an editor might have added
            String footer = matcher.group(3);

            // 3. Reconstruct with proper Unix newlines (\n)
            return header + "\n" + content + "\n" + footer;
        }

        return rawKey;
    }

    /**
     * Connects to the remote server and authenticates the SSH session.
     * @throws IOException If an error occurs during connection or authentication.
     * @throws GeneralSecurityException If an error occurs during security configuration.
     */
    public void connect() throws IOException, GeneralSecurityException {
        // Opens authenticated SSH session to remote host

        // 1. Prepare the input stream
        InputStream keyStream = new ByteArrayInputStream(reformatSSHKey(slurmConfig.sshKeyArmoredString).getBytes(StandardCharsets.UTF_8));

        // 2. Define the Password Provider
        // The provider receives (SessionContext, NamedResource, int retryIndex)
        FilePasswordProvider passwordProvider = (session, resource, retryIndex) -> slurmConfig.sshKeyPassword;

        // 3. Load the KeyPair
        Iterable<KeyPair> keyPairs = SecurityUtils.loadKeyPairIdentities(
                null,
                null,
                keyStream,
                (slurmConfig.sshKeyPassword == null || slurmConfig.sshKeyPassword.isEmpty()) ? null : passwordProvider
        );

        client = SshClient.setUpDefaultClient();
        try {
            // Wrap the key pairs in a custom provider to only use the provided keys
            KeyIdentityProvider wrappedProvider = KeyIdentityProvider.wrapKeyPairs(keyPairs);

            // CRITICAL: Set the KeyIdentityProvider BEFORE starting the client
            // This must replace the default provider to prevent SSHD from loading ~/.ssh keys
            client.setKeyIdentityProvider(wrappedProvider);

            client.start();

            // Configure server key verification
            client.setServerKeyVerifier((clientSession, remoteAddress, serverKey) -> {
                String serverKeyAlgorithm = serverKey.getAlgorithm();
                String encodedKey = Base64.getEncoder().encodeToString(serverKey.getEncoded());

                if (slurmConfig.serverKeyType != null && slurmConfig.serverKey != null &&
                        serverKeyAlgorithm.equals(slurmConfig.serverKeyType) && encodedKey.equals(slurmConfig.serverKey)) {
                    return true;
                }

                if (slurmConfig.serverKeyType == null || slurmConfig.serverKey == null || slurmConfig.serverKey.isEmpty()) {
                    logger.warn("No server key configured. Trusting the server automatically (not recommended for production).");
                    return true;
                }

                logger.error(
                        "Unknown host. Use the following configuration in the slurm configuration if you trust this server:\n" + " serverKeyType:\"{}\"\n serverKey:\"{}\"",
                        serverKeyAlgorithm, encodedKey);
                return false;
            });

            session = client.connect(slurmConfig.user, slurmConfig.host, slurmConfig.port)
                    .verify(SFTP_TIMEOUT).getSession();

            // CRITICAL: Set the identity provider on the session BEFORE authentication
            // This ensures only our provided keys are used during authentication
            session.setKeyIdentityProvider(wrappedProvider);

            // Authenticate using ONLY publickey method with our provided keys
            // This prevents SSHD from trying other authentication methods or loading default keys
            session.auth().verify(SFTP_TIMEOUT);
            logger.info("Successfully connected to the server {}", slurmConfig.host);
        } catch (Throwable t) {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (session != null) {
                session.close();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public ClientSession getSession() {
        return session;
    }


    /**
     * Executes a command on a remote server using the active SSH client session and captures the output.
     *
     * @param command The command string to be executed on the remote server.
     * @param logResult Indicates whether the output of the command should be logged.
     * @param readBytes An {@code AtomicLong} instance that will be updated to reflect the number of bytes read during execution.
     * @param timeoutMs The timeout in milliseconds for the execution of the command (0 for unlimited).
     * @return A list of strings, where each string represents a line of output from the executed command.
     * @throws IOException If an error occurs during command execution or communication over the SSH channel.
     */
    public List<String> runCommand(String command, boolean logResult, AtomicLong readBytes, long timeoutMs)
            throws IOException {
        if (session == null || !session.isAuthenticated()) {
            throw new IOException("SSH session is not connected or authenticated.");
        }
        List<String> lines = new ArrayList<>();
        try (ChannelExec shell = session.createExecChannel(command)) {
            shell.setRedirectErrorStream(false);
            shell.setErr(new LoggingOutputStream(logger, Level.ERROR));
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
                    break;
                }
            }
            shell.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), timeoutMs > 0 ? timeoutMs : 0);
            Integer exitStatus = shell.getExitStatus();
            if (exitStatus != null && exitStatus != 0) {
                logger.error(String.format("Command %s \n exit-status: %d", command, exitStatus));
            }
            readBytes.addAndGet(countingInputStream.getCount());
        }

        return lines;
    }

    /**
     * Executes a command on a remote server using the active SSH client session and captures the output.
     * Use a default timeout of {@link #SFTP_TIMEOUT}.
     *
     * @param command The command string to be executed on the remote server.
     * @param logResult Indicates whether the output of the command should be logged.
     * @return A list of strings, where each string represents a line of output from the executed command.
     * @throws IOException If an error occurs during command execution or communication over the SSH channel.
     */
    public List<String> runCommand(String command, boolean logResult)  throws IOException {
        return runCommand(command, logResult, new AtomicLong());
    }

    /**
     * Executes a command on a remote server using the active SSH client session and captures the output.
     * Use a default timeout of {@link #SFTP_TIMEOUT}.
     *
     * @param command The command string to be executed on the remote server.
     * @param logResult Indicates whether the output of the command should be logged.
     * @param readBytes An {@code AtomicLong} instance that will be updated to reflect the number of bytes read during execution.
     * @return A list of strings, where each string represents a line of output from the executed command.
     * @throws IOException If an error occurs during command execution or communication over the SSH channel.
     */
    public List<String> runCommand(String command, boolean logResult, AtomicLong readBytes)
            throws IOException {
        return runCommand(command, logResult, readBytes, SFTP_TIMEOUT);
    }

    public Logger  getLogger() {
        return logger;
    }


    /**
     * Fetch the {@link SlurmConfig#jobId} progression
     * @param slurmJobProgress
     * @param taskIdToTaskState a map that will be updated with the latest status of each task. The key is the task id and the value is the latest status of the task.
     * @return True if the main job (all the tasks) is finished (succeed or error)
     * @throws IOException
     * @throws CancellationException
     */
    public boolean updateSlurmJobProgression(ProgressVisitor slurmJobProgress, Map<Integer, SlurmJobStatus> taskIdToTaskState) throws IOException, CancellationException {
        List<String> output = runCommand(String.format("scontrol show job %d", slurmConfig.jobId), false);
        List<SlurmJobStatus> jobStatusList = SlurmUtilities.parseSlurmStatus(output);
        for(SlurmJobStatus s : jobStatusList) {
            boolean oldFinished = taskIdToTaskState.containsKey(s.taskId) &&
                    slurmStateMap.containsKey(taskIdToTaskState.get(s.taskId).status) &&
                    slurmStateMap.get(taskIdToTaskState.get(s.taskId).status).finished;
            taskIdToTaskState.merge(s.taskId, s, (oldStatus, newStatus) -> newStatus);
            if(!oldFinished && slurmStateMap.containsKey(s.status) && slurmStateMap.get(s.status).finished) {
                // If the task is finished and was not finished before, increase the progress
                slurmJobProgress.endStep();
            }
            if(slurmStateMap.containsKey(s.status) && slurmStateMap.get(s.status).error) {
                // If one of the process fail, cancel the computation and set the computation as failed
                runCommand(String.format("scancel %d", slurmConfig.jobId), false);
                throw new CancellationException("One of the slurm task has failed and the job has been canceled");
            }
        }
        // If all tasks are Queued print the current status of the tasks:
        if (!taskIdToTaskState.isEmpty() && taskIdToTaskState.values().stream()
                .allMatch(s -> "PENDING".equals(s.status))) {
            logger.info("All tasks are currently queued (PENDING). Current status:");
            for (Map.Entry<Integer, SlurmJobStatus> entry : taskIdToTaskState.entrySet()) {
                logger.info("Task {}: {}", entry.getKey(), entry.getValue().status);
            }
        }

        // If all the tasks are finished, return true. If there is no task returned, we consider that the job is finished.
        // (we cannot update the taskIdToTaskState map if there is no task returned by the command, so we cannot know if the job is finished or not,
        // but we can consider that it is finished because it has been cleaned by slurm)
        return taskIdToTaskState.size() == slurmConfig.maxTasksPerJobs && (taskIdToTaskState.values().stream()
                .allMatch(s -> slurmStateMap.containsKey(s.status)
                        && slurmStateMap.get(s.status).finished) || jobStatusList.isEmpty());
    }
}
