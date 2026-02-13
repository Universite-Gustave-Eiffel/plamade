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
 * Represents a known state of a SLURM (Simple Linux Utility for Resource Management) job.
 * This class is used to define the status, completion state, and error state of a job in the SLURM workload manager.
 */
public class SlurmJobKnownStatus {
    public final String status;
    public final boolean finished;
    public final boolean error;

    /**
     * Constructs a new instance of SlurmJobKnownStatus, representing a known state of a SLURM job.
     *
     * @param status The textual representation of the job's state (e.g., "COMPLETED", "FAILED").
     * @param finished Indicates whether the job has finished execution.
     * @param error Indicates whether the job encountered an error during execution.
     */
    public SlurmJobKnownStatus(String status, boolean finished, boolean error) {
        this.status = status;
        this.finished = finished;
        this.error = error;
    }
}
