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
import net.opengis.wps10.DataInputsType1;
import net.opengis.wps10.ExecuteType;
import net.opengis.wps10.InputType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents the description for a script, with expected inputs and outputs
 */
public class ScriptMetadata {

    final public String id;
    final public String title;
    final public String description;
    final public Path path;

    final public Map<String, ScriptInput> inputs = new HashMap<>();
    final public Map<String, ScriptOutput> outputs = new HashMap<>();

    public ScriptMetadata(String group, File file) throws IOException {
        Map metadata = parseGroovyScriptMetadata(file);
        id = group + ":" + file.getName().replace(".groovy", "");
        title = metadata.getOrDefault("title", id).toString();
        description = metadata.getOrDefault("description", "").toString();
        path = file.toPath();

        // Convert metadata inputs into ScriptInput instances
        Object inputsValue = metadata.get("inputs");
        if(inputsValue instanceof Map) {
            for (Map.Entry input : ((Map<String, Object>) inputsValue).entrySet()) {
                ScriptInput si = new ScriptInput();
                si.id = input.getKey().toString();
                if(input.getValue() instanceof Map) {
                    Map inputAttributes = (Map)input.getValue();
                    si.title = inputAttributes.getOrDefault("title", input.getKey()).toString();
                    si.description = inputAttributes.getOrDefault("description", "").toString();
                    Object typeObj = inputAttributes.get("type");
                    si.type = (typeObj != null) ? typeObj.toString() : "String";
                    Object minValue = inputAttributes.get("min");

                    if (minValue != null) {
                        si.optional = true;
                    }
                }
                inputs.put(si.id, si);
            }
        }

        Object outputsValue = metadata.get("outputs");
        if(outputsValue instanceof Map) {
            for (Map.Entry output : ((Map<String, Object>) outputsValue).entrySet()) {
                ScriptOutput si = new ScriptOutput();
                si.id = output.getKey().toString();
                if(output.getValue() instanceof Map) {
                    si.title = ((Map)output.getValue()).getOrDefault("title", output.getKey()).toString();
                }
                outputs.put(si.id, si);
            }
        }
    }

    /**
     * Parses metadata from a provided Groovy script file and extracts details such as title,
     * description, inputs, and outputs defined within the script. The method analyzes the script
     * content to populate a metadata map, which includes blocks of inputs and outputs if defined.
     *
     * @param scriptFile the Groovy script file to parse for metadata
     * @return a map containing metadata fields such as "title", "description", "inputs", and "outputs",
     * where "inputs" and "outputs" are themselves maps with their respective properties
     * @throws IOException if an error occurs while reading the script file
     */
    private static Map parseGroovyScriptMetadata(File scriptFile) throws IOException {
        GroovyShell shell = new GroovyShell();
        Script script = shell.parse(scriptFile);
        script.run();
        return script.getBinding().getVariables();
    }

    /**
     * Extracts input data from the provided {@code ExecuteType} object and returns a map of input names
     * to their corresponding values. It processes the data inputs defined in the {@code ExecuteType}
     * object to construct this mapping.
     *
     * @param execute the {@code ExecuteType} object that contains the data inputs to extract.
     * @return a map where keys are input names (as strings) and values are the corresponding input data
     *         values (as objects), or {@code null} if no data is associated with an input.
     */
    public static Map<String, Object> extractInputs(ExecuteType execute) {
        Map<String, Object> inputsMap = new HashMap<>();
        DataInputsType1 dataInputs = execute.getDataInputs();
        if (dataInputs != null) {
            for (Object obj : dataInputs.getInput()) {
                if (obj instanceof InputType) {
                    InputType input = (InputType) obj;
                    String name = input.getIdentifier().getValue();
                    Object value = (input.getData() != null && input.getData().getLiteralData() != null)
                            ? input.getData().getLiteralData().getValue() : null;
                    inputsMap.put(name, value);
                }
            }
        }
        return inputsMap;
    }

}

