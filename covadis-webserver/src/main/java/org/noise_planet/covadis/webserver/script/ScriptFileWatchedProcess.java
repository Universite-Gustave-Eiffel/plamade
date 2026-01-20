/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.script;


import org.noise_planet.covadis.webserver.OwsController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Monitors a specified directory and its subdirectories for changes in files.
 * Specifically watches for creation, deletion, and modification events of files with the `.groovy` extension
 * and triggers a script reload using the provided OwsController.
 */
public class ScriptFileWatchedProcess implements Callable<Boolean> {

    private final Path scriptsDir;
    private final OwsController owsController;
    private final Logger logger = LoggerFactory.getLogger(ScriptFileWatchedProcess.class);

    public ScriptFileWatchedProcess(Path scriptsDir, OwsController owsController) {
        this.scriptsDir = scriptsDir;
        this.owsController = owsController;
    }
    @Override
    public Boolean call() throws Exception {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            try (Stream<Path> pathStream = Files.walk(scriptsDir)) {
                pathStream.filter(Files::isDirectory)
                        .forEach(dir -> {
                            try {
                                dir.register(watchService,
                                        StandardWatchEventKinds.ENTRY_CREATE,
                                        StandardWatchEventKinds.ENTRY_DELETE,
                                        StandardWatchEventKinds.ENTRY_MODIFY);
                            } catch (IOException e) {
                                logger.error(e.getMessage(), e);
                            }
                        });
            }
            while (true) {
                try {
                    WatchKey key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path fileName = (Path) event.context();
                        if (fileName.toString().endsWith(".groovy")) {
                            owsController.reloadScripts();
                        }
                    }
                    boolean valid = key.reset();
                    if (!valid) {
                        break;
                    }
                } catch (InterruptedException cwse) {
                    // ignore
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            }
        }
        return true;
    }
}