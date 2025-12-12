/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import io.javalin.http.Context;
import net.opengis.ows11.ExceptionReportType;
import net.opengis.ows11.ExceptionType;
import net.opengis.ows11.Ows11Factory;
import net.opengis.wps10.ExecuteType;
import net.opengis.wps10.ProcessFailedType;
import net.opengis.wps10.Wps10Factory;
import org.geotools.ows.v1_1.OWS;
import org.geotools.ows.v1_1.OWSConfiguration;
import org.geotools.wps.WPSConfiguration;
import org.geotools.xsd.Encoder;
import org.geotools.xsd.Parser;
import org.noise_planet.covadis.webserver.script.Job;
import org.noise_planet.covadis.webserver.script.JobExecutorService;
import org.noise_planet.covadis.webserver.script.ScriptMetadata;
import org.noise_planet.covadis.webserver.script.WpsScriptWrapper;
import org.noise_planet.covadis.webserver.secure.User;
import org.noise_planet.covadis.webserver.utilities.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.h2.server.web.PageParser.escapeHtml;

/**
 * The OwsController class handles requests for OGC Web Services (OWS), including
 * WPS (Web Processing Service), WFS (Web Feature Service), and WCS (Web Coverage Service).
 * It provides functionalities for GET and POST requests, depending on the OWS service and
 * operation type.
 */
public class OwsController {
    public static final int JOB_EXECUTION_TIMEOUT_MS = 5000;
    public static final int CORE_POOL_SIZE = 5;
    public static final int MAXIMUM_POOL_SIZE = 5;
    public static final long KEEP_ALIVE_TIME = 0L;
    private final Logger logger = LoggerFactory.getLogger(OwsController.class);
    UserController userController;
    Configuration configuration;
    DataSource serverDataSource;
    /**
     * Handle threads
     */
    final JobExecutorService jobExecutorService = new JobExecutorService(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE,
            KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS);

    /**
     * A static collection of {@link ScriptMetadata} objects representing the
     * scripts available for the Web Processing Service (WPS). Each script is wrapped
     * in a {@link ScriptMetadata}, which encapsulates its metadata, inputs, outputs,
     * and logic to facilitate execution.
     * <p>
     * This list serves as a central repository of scripts that can be dynamically
     * reloaded and used to process WPS requests. It plays a crucial role in
     * handling WPS operations by mapping process identifiers to their corresponding
     * Groovy script implementations.
     */
    static List<ScriptMetadata> wpsScripts;
    /**
     * An instance of WpsScriptWrapper used to manage the execution of WPS (Web Processing Service) scripts.
     * This wrapper facilitates the interaction between the application and the underlying scripting engine
     * to execute processes defined in scripts. It is responsible for script execution and handling inputs
     * and outputs for WPS processes.
     */
    WpsScriptWrapper wpsScriptWrapper;


    /**
     * Constructs an instance of the OwsController class. This constructor initializes the
     * WPS scripts by loading and grouping them using the WpsScriptWrapper utility. The scripts
     * are classified based on their directory structure and wrapped into appropriate script
     * wrappers for further processing.
     *
     * @throws IOException if an error occurs while loading or processing the script files.
     */
    public OwsController(UserController userController, Configuration configuration, DataSource serverDataSource) throws IOException {
        wpsScriptWrapper = new WpsScriptWrapper(Path.of(configuration.scriptPath));
        Map<String, List<File>> groupedScripts = wpsScriptWrapper.loadScripts();
        wpsScripts = WpsScriptWrapper.buildScriptWrappers(groupedScripts);
        this.userController = userController;
        this.configuration = configuration;
        this.serverDataSource = serverDataSource;
    }
    /**
     * Reloads the WPS (Web Processing Service) scripts by reloading them from the file system
     * and rebuilding the corresponding script wrappers.
     *
     * This method uses {@link WpsScriptWrapper#loadScripts()} to scan and organize script files into
     * groups. The results are then processed by {@link WpsScriptWrapper#buildScriptWrappers(Map)} to
     * create a new set of script wrappers, which replace the existing ones.
     *
     * @throws IOException if an error occurs while loading or rebuilding the scripts.
     */
    public void reloadScripts() throws IOException {
        Map<String, List<File>> groupedScripts = wpsScriptWrapper.loadScripts();
        wpsScripts = WpsScriptWrapper.buildScriptWrappers(groupedScripts);
    }

    /**
     * Handles GET requests for the OWS (Web Services) endpoint. Based on the "service" query parameter,
     * it routes the request to the appropriate WPS, WFS, or WCS service handler. If the service type is unknown,
     * responds with HTTP 400 (Bad Request). Handles exceptions and responds with HTTP 500 (Internal Server Error)
     * in case of server-side errors.
     *
     * @param ctx the context of the current HTTP request, providing access to request parameters,
     *            response handling, and the ability to set content type and status codes
     */
    public void handleGet(Context ctx) {
        ctx.contentType("text/xml; charset=UTF-8");
        String service = ctx.queryParam("service");

        try {
            if ("WPS".equalsIgnoreCase(service)) {
                handleWPSGet(ctx);
            } else if ("WFS".equalsIgnoreCase(service)) {
                handleWFSGet(ctx);
            } else if ("WCS".equalsIgnoreCase(service)) {
                handleWCSGet(ctx);
            } else {
                ctx.status(400).result("Unknown service");
            }
        } catch (Exception e) {
            logger.error("Error handling OWS GET request", e);
            try {
                returnExceptionDocument(ctx, e);
            } catch (IOException ex) {
                logger.error("Error generating error document", ex);
            }
        }
    }


    public void returnExceptionDocument(Context ctx, Exception ex) throws IOException {
        ExceptionReportType report = generateExceptionDocument(ex);

        ProcessFailedType failedType = Wps10Factory.eINSTANCE.createProcessFailedType();
        failedType.setExceptionReport(report);

        Encoder encoder = new Encoder(new OWSConfiguration());
        encoder.setIndenting(true);
        encoder.setIndentSize(2);

        // used to throw an exception here
        ctx.status(500).result(encoder.encodeAsString(report, OWS.ExceptionReport));
    }

    public ExceptionReportType generateExceptionDocument(Exception ex) {
        ExceptionType e = Ows11Factory.eINSTANCE.createExceptionType();
        e.setExceptionCode(ex.getMessage());
        e.setLocator(ex.getClass().getName());
        for (StackTraceElement traceElement : ex.getStackTrace()) {
            e.getExceptionText().add(traceElement.toString());
        }
        ExceptionReportType report = Ows11Factory.eINSTANCE.createExceptionReportType();
        report.setVersion("2.0");
        report.getException().add(e);
        return report;
    }

    /**
     * Handles HTTP GET requests for the WPS (Web Processing Service) by managing
     * the "GetCapabilities" and "DescribeProcess" operations. Based on the "request"
     * query parameter, this method retrieves WPS capabilities or details of a specific
     * process. Responds with appropriate XML content or error messages in case of invalid
     * requests or missing parameters.
     *
     * @param ctx the context of the current HTTP request, providing access to
     *            query parameters, response handling, and the ability to set
     *            content type and status codes
     */
    private void handleWPSGet(Context ctx) {
        String request = ctx.queryParam("request");
        ctx.contentType("text/xml; charset=UTF-8");

        if ("GetCapabilities".equalsIgnoreCase(request)) {
            String xml = WpsScriptWrapper.generateCapabilitiesXML(wpsScripts);
            ctx.result(xml);

        } else if ("DescribeProcess".equalsIgnoreCase(request)) {
            String identifier = ctx.queryParam("identifier");
            if (identifier == null || identifier.isEmpty()) {
                ctx.status(400).result("<ows:Exception>Missing identifier parameter</ows:Exception>");
                return;
            }

            Optional<ScriptMetadata> target = wpsScripts.stream()
                    .filter(w -> w.id.equals(identifier))
                    .findFirst();

            if (target.isPresent()) {
                ctx.result(WpsScriptWrapper.generateDescribeProcessXML(target.get()));
            } else {
                ctx.status(404).result("<ows:Exception>Process not found: " + identifier + "</ows:Exception>");
            }

        } else {
            ctx.status(400).result("<ows:Exception>Unknown WPS request: " + request + "</ows:Exception>");
        }
    }

    /**
     * Handles WFS (Web Feature Service) GET requests for the OWS (Web Services) endpoint.
     * Depending on the value of the "request" query parameter, this method determines the desired operation.
     * If the request is "GetCapabilities", the corresponding XML file is read and returned in the response.
     * For unknown or unsupported requests, it returns an HTTP 400 (Bad Request) status.
     *
     * @param ctx the context of the current HTTP request, providing access to query parameters,
     *            request and response handling, and allowing for status and body configuration
     * @throws Exception if an error occurs while reading or responding with the requested resource
     */
    private void handleWFSGet(Context ctx) throws Exception {
        String request = ctx.queryParam("request");
        if ("GetCapabilities".equalsIgnoreCase(request)) {
            try (InputStream xmlStream = getClass().getResourceAsStream("static/xmlFiles/wfs.xml")){
                if (xmlStream != null) {
                    ctx.result(xmlStream.readAllBytes());
                }
            }
        } else {
            ctx.status(400).result("Unknown WFS request");
        }
    }

    /**
     * Handles a Get request for the Web Coverage Service (WCS). The method processes
     * the incoming HTTP request by examining the "request" query parameter. If the
     * query parameter is "GetCapabilities", it responds with the contents of a WCS
     * capabilities XML file. If the request is not recognized, it responds with a
     * 400 HTTP status and an error message.
     *
     * @param ctx the context of the HTTP request, providing access to query parameters,
     *            response handling, status codes, and other request-related information
     * @throws Exception if an I/O error occurs while attempting to read the WCS capabilities
     *                   XML file or while processing the request
     */
    private void handleWCSGet(Context ctx) throws Exception {
        String request = ctx.queryParam("request");
        if ("GetCapabilities".equalsIgnoreCase(request)) {
            try (InputStream xmlStream = getClass().getResourceAsStream("static/xmlFiles/wcs.xml")) {
                if (xmlStream != null) {
                    ctx.result(xmlStream.readAllBytes());
                }
            }
        } else {
            ctx.status(400).result("Unknown WCS request");
        }
    }


    /**
     * Executes a Groovy script with the provided connection and inputs.
     *
     * @param connection an active database connection used in the script execution context.
     * @param inputs a map of key-value pairs representing the inputs required by the script.
     * @return the result of the script execution, which can be of any type.
     * @throws IOException if there is an issue reading or processing the script file.
     */
    public static Object execute(Connection connection, ScriptMetadata scriptMetadata, Map<String, Object> inputs) throws IOException {
        File scriptFile = scriptMetadata.path.toFile();
        GroovyShell shell = new GroovyShell();
        Script script = shell.parse(scriptFile);

        return script.invokeMethod("exec", new Object[]{connection, inputs});
    }

    /**
     * Handles an HTTP POST request for a Web Processing Service (WPS) operation.
     * This method parses the request body, validates the WPS Execute Request, identifies
     * the target script to execute based on its process identifier, and executes the script.
     * The result of the script execution is returned as a JSON response.
     * Responds with appropriate HTTP status codes for invalid requests, missing scripts,
     * and internal server errors.
     *
     * @param ctx the context of the HTTP request, providing access to the request body,
     *            response handling, and the ability to set status codes and send JSON responses
     */
    public void handleWPSPost(Context ctx) {
        try {
            User user = userController.getUser(ctx);
            Parser parser = new Parser(new WPSConfiguration());
            Object parsed = parser.parse(new ByteArrayInputStream(ctx.bodyAsBytes()));

            if (!(parsed instanceof ExecuteType)) {
                ctx.status(400).result("WPS request not valid");
                return;
            }

            ExecuteType execute = (ExecuteType) parsed;
            String processId = execute.getIdentifier().getValue();

            String[] parts = processId.split(":");
            if (parts.length != 2) {
                ctx.status(400).result("Invalid process ID");
                return;
            }

            String group = parts[0];
            String scriptName = parts[1];

            // Fetch expected script
            Optional<ScriptMetadata> optionalScriptMetadata = wpsScripts.stream()
                    .filter(sw -> sw.id.equals(group + ":" + scriptName))
                    .findFirst();

            if (optionalScriptMetadata.isEmpty()) {
                returnExceptionDocument(ctx, new IllegalArgumentException("Invalid script name: " + scriptName));
                return;
            }
            ScriptMetadata scriptMetadata = optionalScriptMetadata.get();
            Map<String, Object> inputs = ScriptMetadata.extractInputs(execute);
            Job<Object> job = new Job<>(user, scriptMetadata, serverDataSource, inputs, configuration);
            Future<Object> result = jobExecutorService.submitJob(job);
            Object jobResult = result.get(JOB_EXECUTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (jobResult == null) {
                ctx.json(Map.of("result", "Long running process, please look at the job output in the table"));
            }
        } catch (Exception e) {
            // If error occurred inside the future, unwrap the ExecutionException
            String stackTrace = Logging.formatThrowableAsHtml(e);
            String html = MessageFormat.format("<html><head>    <style>        body '{' font-family: Arial; margin: " +
                    "20px; '}'        .section '{' margin-bottom: 20px; '}'        .title '{' font-size: 20px; " +
                    "font-weight: bold; margin-bottom: 5px; color:#b30000; '}'        .box '{' border: 1px solid " +
                    "#ccc; padding: 10px; background:#fafafa; '}'        .error '{' color: #b30000; font-weight: " +
                    "bold; '}'    </style></head><body>    <div class=''section''>        <div class=''title''>Error:" +
                    " </div>        <div class=''box''><span class=''error''>{0}</span></div>    </div>    <div " +
                    "class=''section''>        <div class=''title''>Inputs Data</div>        <div " +
                    "class=''box''>{1}</div>    </div></body></html>", stackTrace, escapeHtml(ctx.body()));

            ctx.contentType("text/html; charset=UTF-8");
            ctx.result(html);
        }
    }

}