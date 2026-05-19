/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.covadis.webserver;

import net.opengis.wps10.ExecuteResponseType;
import org.apache.log4j.PropertyConfigurator;
import org.h2.value.ValueBoolean;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.functions.io.shp.SHPRead;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.URL;
import java.net.http.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.noise_planet.covadis.webserver.slurm.SlurmConfig;
import org.noise_planet.noisemodelling.scripts.NoiseModelling.Noise_level_from_source;
import org.noise_planet.noisemodelling.webserver.Configuration;
import org.noise_planet.noisemodelling.webserver.OwsController;
import org.noise_planet.noisemodelling.webserver.utilities.Logging;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NoiseModellingHPCServerHttpTest {

    /**
     * A Javalin instance used to manage the HTTP server lifecycle and handle HTTP routes
     * for the web application during testing.
     *
     * This static variable is initialized and configured in the {@code setUp} method,
     * and is responsible for serving HTTP routes used by the test cases defined in the
     * {@link NoiseModellingHPCServerHttpTest} class.
     *
     * It supports the execution of various HTTP-based operations such as handling requests
     * for WPS capabilities, process descriptions, and WPS execution, as verified in the test methods.
     */
    private static NoiseModellingHPCServer app;

    /**
     * The default port number on which the HTTP server will listen.
     *
     * This constant defines the port number used to establish server connections.
     * It is primarily used during the setup phase of the server and
     * in test cases to ensure proper server communication and resource access.
     *
     * Modifying this value may require corresponding updates in client-side
     * configurations and resource endpoints to maintain compatibility.
     */
    private static final int PORT = 8000;
    /**
     * The base URL for the OWS (OGC Web Services) endpoints used in the test cases.
     * It dynamically constructs the URL using the `localhost` domain and the value
     * of the `PORT` variable defined in the class.
     *
     * This URL serves as the base endpoint for various HTTP requests made during
     * the execution of the test suite and is primarily used for testing capabilities,
     * descriptions, and process execution of the WPS (Web Processing Service).
     */
    private static final String BASE_URL = "http://localhost:" + PORT + "/"+Configuration.DEFAULT_APPLICATION_URL+"/builder/ows";

    /**
     * Sets up the test environment for the HTTP-based tests.
     * This method is executed once before all tests in the test class.
     *
     * During the setup, a Javalin server instance is initialized by invoking the
     * {@code Main.startServer} method with the browser opening disabled. The server
     * instance is assigned to the static field {@code app}.
     *
     * @throws IOException if an I/O error occurs while starting the server.
     */
    @BeforeAll
    public static void setUp(@TempDir Path temporaryDirectory) throws IOException, SQLException {
        Logging.initConsoleLogging();
        Configuration configuration = new Configuration(true);
        configuration.setWorkingDirectory(temporaryDirectory.toString());
        app = new NoiseModellingHPCServer(configuration);
        app.startServer(false);
    }

    /**
     * Tears down the testing environment after all tests have been executed.
     *
     * This method is annotated with {@code @AfterAll}, meaning it will be executed
     * once after all test cases in the test class have been run. It is responsible
     * for performing cleanup operations such as stopping the application instance
     * if it has been initialized during the test setup.
     *
     * If the application instance {@code app} is not null, this method will invoke
     * the {@code stop()} method to cease its operations and release any resources
     * associated with it. This ensures a proper shutdown and prevents resource leaks.
     */
    @AfterAll
    public static void tearDown() {
        if (app != null) {
            app.getJavalinInstance().stop();
        }
    }

    @BeforeEach
    public void clearInstance() throws SQLException {
        if (app != null) {
            try(Connection connection = app.getServerDataSource().getConnection()) {
                connection.createStatement().execute("TRUNCATE TABLE JOBS");
            }
        }
    }

    @Test
    @Order(4)
    void testPostWPSCreateSSHConfiguration() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String requestBody = "<p0:Execute xmlns:p0=\"http://www.opengis.net/wps/1.0.0\" service=\"WPS\" version=\"1.0" +
                ".0\"><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">Slurm:Write_HPC_Settings</p1:Identifier><p0:DataInputs><p0:Input><p1:Identifier " +
                "xmlns:p1=\"http://www.opengis.net/ows/1.1\">ssh_key_type</p1:Identifier><p0:Data><p0:LiteralData>ssh" +
                "-rsa</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis" +
                ".net/ows/1.1\">ssl_key</p1:Identifier><p0:Data><p0:LiteralData>none</p0:LiteralData></p0:Data></p0" +
                ":Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">host</p1:Identifier><p0:Data><p0:LiteralData>localhost</p0:LiteralData></p0:Data></p0:Input><p0" +
                ":Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">user</p1:Identifier><p0:Data><p0:LiteralData>testuser</p0:LiteralData></p0:Data></p0:Input><p0" +
                ":Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">java_binary_path</p1:Identifier><p0:Data><p0:LiteralData>java</p0:LiteralData></p0:Data></p0" +
                ":Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">key</p1:Identifier><p0:Data><p0:LiteralData>-----BEGIN OPENSSH PRIVATE KEY----- " +
                "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn " +
                "NhAAAAAwEAAQAAAQEA3r4jz4h6BCZydEFtM20eFHKPsCNXpCzckmd+0EvwMUdG/Tsi0CkK " +
                "ENOP3pE+VJsKC8zVUCvYc+ysZoOWExaYBkghjhpZBjiUWUIFUOdSE8zkL8+u1kZLrgikVd " +
                "oP5s1pDEVjaimoPUPHwGyBeX95FkDcqbMkTp4Hy5wbLbeEr7Gw3lPtK/vr2r3oJmKRWIdV " +
                "dzA4xYm6yoPvPvQaTCfymj8FZJ2h89EhEPA/x1EK34W/lrGl4kMZ1NCFvu2M6dojt76FD/ " +
                "wpHV/QtSqEQPShKNMnLW/pdluzwzsS4eU2SHl3GpPd01qP0jIjU+AtM45v/WQxjauh12Ns " +
                "9fbrPTRnLwAAA8jP1Gawz9RmsAAAAAdzc2gtcnNhAAABAQDeviPPiHoEJnJ0QW0zbR4Uco " +
                "+wI1ekLNySZ37QS/AxR0b9OyLQKQoQ04/ekT5UmwoLzNVQK9hz7Kxmg5YTFpgGSCGOGlkG " +
                "OJRZQgVQ51ITzOQvz67WRkuuCKRV2g/mzWkMRWNqKag9Q8fAbIF5f3kWQNypsyROngfLnB " +
                "stt4SvsbDeU+0r++vavegmYpFYh1V3MDjFibrKg+8+9BpMJ/KaPwVknaHz0SEQ8D/HUQrf " +
                "hb+WsaXiQxnU0IW+7Yzp2iO3voUP/CkdX9C1KoRA9KEo0yctb+l2W7PDOxLh5TZIeXcak9 " +
                "3TWo/SMiNT4C0zjm/9ZDGNq6HXY2z19us9NGcvAAAAAwEAAQAAAQEAizZLK2og2HcvEXnS " +
                "xlFse1secvejzvg640XL/GN5u1LRC3PqTi9YGywevvwH+NjtbnKW34SHw+wn0+pp4YQ9f6 " +
                "+VSTsuaLT0AtVAfVAV/EoSU895dnJ42kyMaRvg1F+NSB4WBEQE4kV6ksk+IrGI/F+NioJs " +
                "LraWKKtoUSphw2wgC1zD3+g/ReqIHsFjm7ml0z9Uy3lI1mSNY/f8AUNI/ylD+NY7ExOUKr " +
                "TXMjsF0GhlGVMUsOPWzwYABE/l9IAQqrTM8y6ItwLNYg7QaU7M5dJIwt0ImWGHu8vG3pTP " +
                "sLDXsq0+B2OKS/56aruxUMh8NYSQqRWzbvpmIjhL/+AAAQAAAIEAshSK3GTgv0s+2AKTvu " +
                "B37E+a6sB/rq1SAwe1qzMTiCeUTcu4YJryjWb0HAxxxfV8cPlX9KOMdZIQPcnqDYda7Pmf " +
                "GJ48arkfpslhlT837DifEQHUvksbR8WShlHvwJ1EBoTNINhIP46QHexycPMuxzXB35tdNA " +
                "MJZa02dhWA9mQAAACBAP4233osUHyQWxtgcCrgRbXCMDQ+lpefv42KftHsODSlkFVsKoZv " +
                "FzF1XGXDDSSrNqEiYtjtArmKWicZB0iQHp4hhrZ59Ne6Bxvu3GUaHk//vUNfJkDGu4VrUF " +
                "/4ojV6mcwJibyoROwpbz3muHd4bMLZ6WGWbT0T9/pw37Pwl2VzAAAAgQDgTqzBBP5wmh/p " +
                "g+MvfN+dOffEdalpEa71QGyu0SQT8MSq9RX7qQjkBLijxcBZVThicY6DVUxT+RUTYjMz7E " +
                "t9UciovHTxjaytvodoAD36bn2ZRg0v8imS9ct7cknp+EU3PpvTPUZUKYGn+ARVpyuYWd/5 " +
                "R83Qu+7qV0ztWSNoVQAAABB0ZXN0QGV4YW1wbGUuY29tAQ== -----END OPENSSH PRIVATE " +
                "KEY-----</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis" +
                ".net/ows/1.1\">configuration_name</p1:Identifier><p0:Data><p0:LiteralData>local</p0:LiteralData></p0" +
                ":Data></p0:Input></p0:DataInputs><p0:ResponseForm><p0:RawDataOutput><p1:Identifier " +
                "xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">result</p1:Identifier></p0:RawDataOutput></p0:ResponseForm></p0:Execute>";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Content-Type", "text/xml")
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());

        try(Connection connection = app.getUserDataSource(1).getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM SLURM_CONFIGURATION")) {
                try(ResultSet rs = preparedStatement.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("local", rs.getString("configuration_name"));
                }
            }
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testWPSRunRemoteNoiseModelling() throws Exception {
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

        // Register SLURM configuration for docker slurm instance running on localhost with key ssh access
        HttpClient client = HttpClient.newHttpClient();
        String requestBody = String.format("<p0:Execute xmlns:p0=\"http://www.opengis.net/wps/1.0.0\" service=\"WPS\" version=\"1.0.0\"><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">Slurm:Write_HPC_Settings</p1:Identifier><p0:DataInputs><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">ssh_key_type</p1:Identifier><p0:Data><p0:LiteralData>ssh-rsa</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">ssl_key</p1:Identifier><p0:Data><p0:LiteralData></p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">host</p1:Identifier><p0:Data><p0:LiteralData>%s</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">user</p1:Identifier><p0:Data><p0:LiteralData>%s</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">java_binary_path</p1:Identifier><p0:Data><p0:LiteralData>/usr/lib/jvm/java-21-openjdk-amd64/bin/java</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">key</p1:Identifier><p0:Data><p0:LiteralData>%s</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">configuration_name</p1:Identifier><p0:Data><p0:LiteralData>local</p0:LiteralData></p0:Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">port</p1:Identifier><p0:Data><p0:LiteralData>%d</p0:LiteralData></p0:Data></p0:Input></p0:DataInputs><p0:ResponseForm><p0:RawDataOutput><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1.1\">result</p1:Identifier></p0:RawDataOutput></p0:ResponseForm></p0:Execute>", config.host, config.user, config.sshKeyArmoredString, config.port);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Content-Type", "text/xml")
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        // Load unit test input data tables
        int expectedRows = 0;
        try(Connection connection = app.getUserDataSource(1).getConnection()) {
            URL url = NoiseModellingHPCServerHttpTest.class.getResource("buildings.shp");
            assertNotNull(url);
            SHPRead.importTable(connection, url.getFile(),"BUILDINGS" ,ValueBoolean.TRUE);
            url = NoiseModellingHPCServerHttpTest.class.getResource("lw_roads.shp");
            assertNotNull(url);
            SHPRead.importTable(connection, url.getFile(),"SOURCES" ,ValueBoolean.TRUE);
            url = NoiseModellingHPCServerHttpTest.class.getResource("receivers.shp");
            assertNotNull(url);
            SHPRead.importTable(connection, url.getFile(),"RECEIVERS" ,ValueBoolean.TRUE);
            // Run local noise modelling to get the expected number of rows in RECEIVERS LEVEL
            Map<String, Object> input = new HashMap<>();
            input.put("tableSources", "SOURCES");
            input.put("tableBuilding", "BUILDINGS");
            input.put("tableReceivers", "RECEIVERS");
            input.put("frequencyFieldPrepend", "LW");
            input.put("confExportReceiverGeometry", false);
            input.put("confExportSourceId", true);

            new Noise_level_from_source().exec(connection, input, new EmptyProgressVisitor());

            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(DISTINCT IDRECEIVER) FROM RECEIVERS_LEVEL")) {
                try(ResultSet rs = preparedStatement.executeQuery()) {
                    assertTrue(rs.next());
                    expectedRows = rs.getInt(1);
                }
            }
            // Drop receivers table
            try(Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE RECEIVERS_LEVEL");
            }
        }

        URL xmlQuery = NoiseModellingHPCServerHttpTest.class.getResource("wps_parse/slurmNoiseLevelFromSource.xml");
        assertNotNull(xmlQuery);
        try(InputStream inputStream = xmlQuery.openStream()) {
            requestBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Content-Type", "text/xml")
                .build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        ExecuteResponseType executeResponseType = OwsController.parseExecuteResponse(new ByteArrayInputStream(response.body().getBytes()));
        String requestResponseUrl = executeResponseType.getStatusLocation();

        // Polls status endpoint until process completes or fails
        while (!(executeResponseType.getStatus().getProcessFailed() != null ||
                executeResponseType.getStatus().getProcessSucceeded() != null)) {
            Thread.sleep(500);
            request = HttpRequest.newBuilder()
                    .uri(URI.create(requestResponseUrl))
                    .GET()
                    .build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            executeResponseType = OwsController.parseExecuteResponse(new ByteArrayInputStream(response.body().getBytes()));
        }

        // Polls database for table existence and row count
        try(Connection connection = app.getUserDataSource(1).getConnection()) {
            Statement statement = connection.createStatement();
            try(ResultSet resultSet = statement.executeQuery("SELECT COUNT(DISTINCT IDRECEIVER) FROM RECEIVERS_LEVEL")) {
                assertTrue(resultSet.next());
                int rowCount = resultSet.getInt(1);
                assertEquals(expectedRows, rowCount);
            }
        }

    }
}
