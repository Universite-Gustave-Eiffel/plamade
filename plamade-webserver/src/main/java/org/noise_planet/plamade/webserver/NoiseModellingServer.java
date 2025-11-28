package org.noise_planet.plamade.webserver;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class NoiseModellingServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoiseModellingServer.class);
    private Javalin app;
    private Future<?> scriptWatch;
    /**
     * The entry point of the application. This method initializes and starts the server.
     *
     * @param args command-line arguments passed to the application. Not utilized currently.
     * @throws IOException if an I/O error occurs during server initialization.
     */
    public static void main(String[] args) throws IOException {
        NoiseModellingServer noiseModellingServer = new NoiseModellingServer();
        noiseModellingServer.startServer(true);
    }

    /**
     * @return Returns the Javalin application instance.
     */
    public Javalin getJavalinInstance() {
        return app;
    }

    /**
     * Initializes and starts the NoiseModelling server with the specified configuration.
     * The server serves static files, provides endpoints for OGC-compliant operations,
     * and optionally opens a browser pointing to the server's base URL.
     *
     * @param openBrowser indicates whether the default web browser should be opened
     *                     pointing to the server's base URL.
     * @return the initialized and started Javalin application instance.
     * @throws IOException if an I/O error occurs during server initialization or script directory resolution.
     */
    public Javalin startServer(boolean openBrowser) throws IOException {
        Path scriptsDir = WpsScriptWrapper.findScriptsDir();

        PropertyConfigurator.configure(
                Objects.requireNonNull(NoiseModellingServer.class.getResource("static/log4j.properties")));

        OwsController owsController = new OwsController();

        File htmlWpsBuilderPath;
        try {
            htmlWpsBuilderPath = new File(Objects.requireNonNull(NoiseModellingServer.class.getResource("static/wpsbuilder"))
                    .toURI());
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
        app = Javalin.create(config -> {
            config.staticFiles.add(htmlWpsBuilderPath.getAbsolutePath(),
                    Location.EXTERNAL);
        }).start(8000);

        int port = app.port();
        String url = "http://localhost:" + port + "/";
        LOGGER.info("Start NoiseModelling: " + url);

        if (openBrowser) {
            openBrowser(url);
        }

        app.get("/ows", owsController::handleGet);
        app.post("/ows", owsController::handleWPSPost);

        scriptWatch = startWatcher(scriptsDir, owsController);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // Stop watching for script changes
                scriptWatch.cancel(true);
                app.stop();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }));

        return app;
    }

    /**
     * Opens the default web browser and navigates to the specified URL.
     *
     * @param url the URL to be opened in the default web browser. It must be a properly formatted URI.
     */
    public void openBrowser(String url) {
        try {
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(url));
            }
        } catch (Exception e) {
            LOGGER.error("Unable to open the browser : {}", e.getMessage(), e);
        }
    }

    /**
     * Monitors a specified directory and its subdirectories for changes in files.
     * Specifically watches for creation, deletion, and modification events of files with the `.groovy` extension
     * and triggers a script reload using the provided OwsController.
     */
    public static final class ScriptFileWatchedProcess implements Callable<Boolean> {

        private final Path scriptsDir;
        private final OwsController owsController;

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
                                    LOGGER.error(e.getMessage(), e);
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
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                        return false;
                    }
                }
            }
            return true;
        }
    }
    /**
     * Monitors a specified directory and its subdirectories for changes in files.
     * Specifically watches for creation, deletion, and modification events of files with the `.groovy` extension
     * and triggers a script reload using the provided OwsController.
     *
     * @param scriptsDir the root directory to monitor for changes. All subdirectories under this will also be monitored.
     * @param owsController the instance responsible for reloading scripts when a `.groovy` file is changed.
     * @return a Future representing the asynchronous script reload operation.
     */
    private Future<Boolean> startWatcher(Path scriptsDir, OwsController owsController) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<Boolean> task = new ScriptFileWatchedProcess(scriptsDir, owsController);
        return executor.submit(task);
    }
}
