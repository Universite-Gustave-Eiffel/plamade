/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 * <p>
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Contact: contact@noise-planet.org
 *
 */
package org.noise_planet.plamade.webserver;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.apache.log4j.PropertyConfigurator;
import org.noise_planet.plamade.webserver.secure.Auth;
import org.noise_planet.plamade.webserver.secure.Role;
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

public class NoiseModellingServer {
    private final Logger logger = LoggerFactory.getLogger(NoiseModellingServer.class);
    private Javalin app;
    private Future<?> scriptWatch;
    private final OwsController owsController;
    private final Configuration configuration;

    public NoiseModellingServer(Configuration configuration) throws IOException {
        this.configuration = configuration;
        owsController  = new OwsController(Path.of(configuration.scriptPath));
    }

    /**
     * The entry point of the application. This method initializes and starts the server.
     *
     * @param args command-line arguments passed to the application. Not utilized currently.
     * @throws IOException if an I/O error occurs during server initialization.
     */
    public static void main(String[] args) throws IOException {
        PropertyConfigurator.configure(
                Objects.requireNonNull(NoiseModellingServer.class.getResource("static/log4j.properties")));
        Configuration configuration = Configuration.createConfigurationFromArguments(args);
        NoiseModellingServer noiseModellingServer = new NoiseModellingServer(configuration);
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
     *                    pointing to the server's base URL.
     * @throws IOException if an I/O error occurs during server initialization or script directory resolution.
     */
    public void startServer(boolean openBrowser) throws IOException {
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
        });

        app.beforeMatched(Auth::handleAccess);

        int port = app.port();
        String url = "http://localhost:" + port + "/";
        logger.info("Start NoiseModelling: {}", url);

        if (openBrowser) {
            openBrowser(url);
        }

        app.get("/ows", owsController::handleGet, Role.ANYONE);
        app.post("/ows", owsController::handleWPSPost);

        scriptWatch = startWatcher(Path.of(configuration.scriptPath), owsController);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // Stop watching for script changes
                scriptWatch.cancel(true);
                app.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }));

        app.start(8000);
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
            logger.error("Unable to open the browser : {}", e.getMessage(), e);
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
