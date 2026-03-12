/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.covadis.webserver.slurm;

/**
 * Represents the status of a SLURM (Simple Linux Utility for Resource Management) job, including its current state and associated task ID.
 * This class is used to encapsulate the status information of a SLURM job, allowing for easy access to the job's state and task identifier.
 */
public class SlurmJobStatus {
    public final String status;
    public final int taskId;

    public SlurmJobStatus(String status, int taskId) {
        this.status = status;
        this.taskId = taskId;
    }
}
