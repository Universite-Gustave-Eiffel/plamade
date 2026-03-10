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
 * Represents the attributes of a file, including its name and size.
 * This class is used to encapsulate the file attributes, allowing for easy access to the file's name and size information.
 */
public class FileAttributes {
    public final String fileName;
    public final long fileSize;

    /**
     * Constructs a new instance of FileAttributes, representing the attributes of a file.
     *
     * @param fileName The name of the file.
     * @param fileSize The size of the file in bytes.
     */
    public FileAttributes(String fileName, long fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
    }
}
