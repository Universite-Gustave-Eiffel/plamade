/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.script;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.commons.text.StringEscapeUtils;
import org.h2.server.web.PageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The `WpsScriptWrapper` class provides functionalities to manage, organize, and process
 * Groovy scripts for use in a Web Processing Service (WPS) environment. It includes methods
 * for loading scripts, parsing their metadata, grouping them into categories, and generating
 * WPS-compliant XML documents.
 *
 * The class relies on the directory structure of Groovy script files to organize them into
 * groups, and it provides mechanisms for extracting script information, such as inputs,
 * outputs, descriptions, and other metadata. These capabilities facilitate the integration
 * of scripts into a WPS framework by generating necessary XML representations.
 */
public class WpsScriptWrapper {

    /**
     * The root directory where Groovy script files are stored and managed.
     * This variable represents the base directory from which scripts are loaded,
     * grouped, and processed within the WpsScriptWrapper class.
     */
    private Path scriptsRoot;

    /**
     * Default constructor for the WpsScriptWrapper class.
     *
     * This constructor initializes the WpsScriptWrapper instance by setting the
     * `scriptsRoot` field to point to the default directory containing Groovy
     * script files. The directory is resolved relative to the current working
     * directory of the application and is expected to exist at:
     * "noisemodelling-scripts/src/main/groovy/org/noise_planet/noisemodelling/scripts".
     */
    public WpsScriptWrapper(Path scriptDir) {
        this.scriptsRoot = scriptDir;
    }


    /**
     * Loads Groovy scripts from a predefined directory structure and organizes them into groups.
     *
     * This method scans the available scripts using the `scanScriptsGrouped` method to organize
     * them by groups, then attempts to locate the corresponding script files for each script
     * name in the directory structure. Only valid script files that exist on the file system
     * are included in the resulting map.
     *
     * @return a map where the keys are script group names and the values are lists of
     *         File objects corresponding to the scripts in each group
     */
    public  Map<String, List<File>> loadScripts(){
        return scanScriptsGrouped(getClass().getClassLoader(), scriptsRoot);
    }

    /**
     * Scans a predefined directory structure containing Groovy scripts and organizes them into groups.
     * <p>
     * This method traverses the directory structure rooted at the `scriptsRoot` location recursively.
     * It identifies Groovy script files (files ending with the `.groovy` extension), extracts their names
     * (excluding the file extension), and groups them into categories based on the directory structure.
     * Each group corresponds to a directory path relative to the root directory.
     * <p>
     * If the root directory does not exist or contains no valid files, an empty map is returned.
     *
     * @return a map where the keys are group names (relative directory paths) and the values are lists
     *         of script names (without file extensions) belonging to each group
     */
    public static Map<String, List<File>> scanScriptsGrouped(ClassLoader loader, Path scriptDirectory) {
        Map<String, List<File>> grouped = new TreeMap<>();
        File baseDir = scriptDirectory.toFile();
        Logger logger = LoggerFactory.getLogger(WpsScriptWrapper.class.getName());
        logger.info("Scanning scripts in directory: " + scriptDirectory.toAbsolutePath());
        if (!baseDir.exists()) {
            logger.warn("Directory does not exist: " + scriptDirectory);
            // The location may be stored into the jar not the local file system
            try {
                URL resourceUrl = loader.getResource(scriptDirectory.toString());
                if (resourceUrl == null) {
                    return grouped;
                }
                baseDir = new File(resourceUrl.toURI());
            } catch (URISyntaxException e) {
                return grouped;
            }
            if (!baseDir.exists()) {
                return grouped;
            }
        }
        scanRecursive(baseDir, "", grouped);
        return grouped;
    }


    /**
     * Finds a Groovy script file based on the specified group and script name.
     *
     * This method builds the path to the desired script file by resolving the group
     * and script name against a predefined root directory. If the file exists, it
     * returns a {@code File} object representing the script; otherwise, it returns null.
     *
     * @param group the name of the group or folder containing the script
     *              (relative to the root directory)
     * @param scriptName the name of the script file (without the ".groovy" extension)
     * @return a {@code File} object representing the script file if it exists,
     *         or null if the file does not exist
     */
    public File findScript(String group, String scriptName) {
        Path path = scriptsRoot.resolve(group).resolve(scriptName + ".groovy");
        return Files.exists(path) ? path.toFile() : null;
    }

    /**
     * Recursively scans a directory for Groovy script files and groups them into categories
     * based on the directory structure. Each group corresponds to a directory path relative
     * to the root directory.
     *
     * @param dir the directory to scan for Groovy script files
     * @param currentGroup the current group name, representing the relative path from the root directory
     * @param grouped a map where keys are group names (relative directory paths) and values are lists
     *        of script files that belong to each group
     */
    private static void scanRecursive(File dir, String currentGroup, Map<String, List<File>> grouped) {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isDirectory()) {
                String newGroup = currentGroup.isEmpty() ? f.getName() : currentGroup + "/" + f.getName();
                scanRecursive(f, newGroup, grouped);
            } else if (f.getName().endsWith(".groovy")) {
                grouped.computeIfAbsent(currentGroup, k -> new ArrayList<>())
                        .add(f);
            }
        }
    }

    /**
     * Builds a list of {@link ScriptMetadata} objects from the Groovy scripts available
     * in the given directory or JAR.
     *
     * <p>This method reads each Groovy script file, parses its metadata (title,
     * description, inputs, and outputs), and wraps it into a {@link ScriptMetadata}
     * instance. The resulting list can be used to generate WPS Capabilities and
     * DescribeProcess documents.</p>
     *
     * @param scriptFiles a map of grouped script files (group → list of script files)
     * @return a list of {@code ScriptWrapper} instances representing available scripts
     * @throws IOException if a script file cannot be read or parsed
     */
    public static List<ScriptMetadata> buildScriptWrappers(Map<String, List<File>> scriptFiles) throws IOException {
        List<ScriptMetadata> wrappers = new ArrayList<>();
        for (Map.Entry<String, List<File>> entry : scriptFiles.entrySet()) {
            String group = entry.getKey();
            for (File file : entry.getValue()) {
                ScriptMetadata wrapper = new ScriptMetadata(group, file);
                wrappers.add(wrapper);
            }
        }

        return wrappers;
    }

    /**
     * Generates a WPS GetCapabilities XML document listing all available scripts.
     *
     * @param scripts the list of available ScriptWrapper instances
     * @return XML string for WPS GetCapabilities
     */
    public static String generateCapabilitiesXML(List<ScriptMetadata> scripts) {
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

            Element root = doc.createElementNS("http://www.opengis.net/wps/1.0.0", "wps:Capabilities");
            root.setAttribute("xmlns:wps", "http://www.opengis.net/wps/1.0.0");
            root.setAttribute("xmlns:ows", "http://www.opengis.net/ows/1.1");
            root.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
            root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            root.setAttribute("xsi:schemaLocation", "http://www.opengis.net/wps/1.0.0 http://schemas.opengis" +
                    ".net/wps/1.0.0/wpsAll.xsd");
            doc.appendChild(root);

            Element serviceIdentification = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows" +
                    ":ServiceIdentification");
            root.appendChild(serviceIdentification);

            Element title = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Title");
            title.setTextContent("Prototype GeoServer WPS");
            serviceIdentification.appendChild(title);

            Element abstractElem = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Abstract");
            abstractElem.setTextContent("");
            serviceIdentification.appendChild(abstractElem);

            Element serviceType = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:ServiceType");
            serviceType.setTextContent("WPS");
            serviceIdentification.appendChild(serviceType);

            Element serviceTypeVersion = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows" +
                    ":ServiceTypeVersion");
            serviceTypeVersion.setTextContent("1.0.0");
            serviceIdentification.appendChild(serviceTypeVersion);

            Element processOfferings = doc.createElement("wps:ProcessOfferings");
            root.appendChild(processOfferings);

            for (ScriptMetadata script : scripts) {
                Element process = doc.createElement("wps:Process");
                process.setAttribute("wps:processVersion", "1.0.0");
                processOfferings.appendChild(process);

                Element identifier = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Identifier");
                identifier.setTextContent(script.id);
                process.appendChild(identifier);

                Element processTitle = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Title");
                processTitle.setTextContent(script.title);
                process.appendChild(processTitle);

                Element processAbstract = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Abstract");
                processAbstract.setTextContent(escapeForWpsXml(script.description));
                process.appendChild(processAbstract);
            }

            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc),
                    new StreamResult(writer));

            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate XML", e);
        }
    }
    /**
     * Generates a WPS DescribeProcess XML for a specific Groovy script.
     *
     * @param wrapper the ScriptWrapper representing the script
     * @return XML string for WPS DescribeProcess
     */
    public static String generateDescribeProcessXML(ScriptMetadata wrapper) {
        try {
            Document doc =
                    DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

            Element root = doc.createElementNS("http://www.opengis.net/wps/1.0.0", "wps" +
                    ":ProcessDescriptions");
            root.setAttribute("xmlns:wps", "http://www.opengis.net/wps/1.0.0");
            root.setAttribute("xmlns:ows", "http://www.opengis.net/ows/1.1");
            root.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
            root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            root.setAttribute("xsi:schemaLocation", "http://www.opengis.net/wps/1.0.0 http://schemas.opengis" +
                    ".net/wps/1.0.0/wpsAll.xsd");
            root.setAttribute("xml:lang", "en-US");
            doc.appendChild(root);

            Element processDesc = doc.createElement("ProcessDescription");
            processDesc.setAttribute("wps:processVersion", "1.0.0");
            processDesc.setAttribute("storeSupported", "true");
            processDesc.setAttribute("statusSupported", "true");
            root.appendChild(processDesc);

            Element identifier = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Identifier");
            identifier.setTextContent(wrapper.id);
            processDesc.appendChild(identifier);

            Element title = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Title");
            title.setTextContent(wrapper.title);
            processDesc.appendChild(title);

            Element abstractElem = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Abstract");
            abstractElem.setTextContent(wrapper.description);
            processDesc.appendChild(abstractElem);

            Element dataInputs = doc.createElement("DataInputs");
            processDesc.appendChild(dataInputs);

            for (ScriptInput input : wrapper.inputs.values()) {
                Element inputElem = doc.createElement("Input");
                inputElem.setAttribute("minOccurs", input.optional ? "0" : "1");
                inputElem.setAttribute("maxOccurs", "1");
                dataInputs.appendChild(inputElem);

                Element inputId = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Identifier");
                inputId.setTextContent(input.id);
                inputElem.appendChild(inputId);

                Element inputTitle = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Title");
                inputTitle.setTextContent(input.title);
                inputElem.appendChild(inputTitle);

                Element inputAbstract = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows" +
                        ":Abstract");
                inputAbstract.setTextContent(input.description);
                inputElem.appendChild(inputAbstract);

                Element literalData = doc.createElement("LiteralData");
                inputElem.appendChild(literalData);

                Element dataType = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows" +
                        ":DataType");
                if (input.type.equals(Boolean.class)) {
                    dataType.setTextContent("xs:boolean");
                    literalData.appendChild(dataType);

                    Element allowedValues = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows" +
                            ":AllowedValues");
                    literalData.appendChild(allowedValues);

                    Element valueTrue = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Value");
                    valueTrue.setTextContent("true");
                    allowedValues.appendChild(valueTrue);

                    Element valueFalse = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Value");
                    valueFalse.setTextContent("false");
                    allowedValues.appendChild(valueFalse);

                    Element defaultValue = doc.createElement("DefaultValue");
                    defaultValue.setTextContent("false");
                    literalData.appendChild(defaultValue);
                } else {
                    dataType.setTextContent("xs:" + input.type.getSimpleName().toLowerCase(Locale.ROOT));
                    literalData.appendChild(dataType);
                }
            }

            Element processOutputs = doc.createElement("ProcessOutputs");
            processDesc.appendChild(processOutputs);

            for (ScriptOutput output : wrapper.outputs.values()) {
                Element outputElem = doc.createElement("Output");
                processOutputs.appendChild(outputElem);

                Element outputId = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Identifier");
                outputId.setTextContent(output.id);
                outputElem.appendChild(outputId);

                Element outputTitle = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:Title");
                outputTitle.setTextContent(output.title);
                outputElem.appendChild(outputTitle);

                Element literalOutput = doc.createElement("LiteralOutput");
                outputElem.appendChild(literalOutput);

                Element dataType = doc.createElementNS("http://www.opengis.net/ows/1.1", "ows:DataType");
                dataType.setTextContent("String");
                literalOutput.appendChild(dataType);
            }

            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc),
                    new StreamResult(writer));

            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate XML", e);
        }
    }


    /**
     * Escapes a given input string for use in WPS (Web Processing Service) XML documents.
     * This method sanitizes the input by removing HTML tags, replacing certain characters
     * with their XML entity equivalents, and trimming any leading or trailing whitespace.
     *
     * @param input the input string to be escaped; can be null
     * @return the escaped string suitable for inclusion in a WPS XML document;
     *         returns an empty string if the input is null
     */
    private static String escapeForWpsXml(String input) {
        if (input == null) return "";
        // Escape HTML tags
        return StringEscapeUtils.escapeHtml4(input).trim();
    }


}


