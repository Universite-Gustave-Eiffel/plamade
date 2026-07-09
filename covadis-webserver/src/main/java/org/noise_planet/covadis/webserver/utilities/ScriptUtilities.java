package org.noise_planet.covadis.webserver.utilities;

import groovy.lang.MetaMethod;
import groovy.lang.Script;
import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.noise_planet.noisemodelling.webserver.script.ExecutionPlan;
import org.noise_planet.noisemodelling.webserver.script.ScriptMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptUtilities {

    public static Object execScript(Script script, Connection connection, Map inputs) {
        return execScript(script, connection, inputs, new EmptyProgressVisitor());
    }

    public static Object execScript(Script script, Connection connection, Map inputs, ProgressVisitor progress) {
        Logger logger = LoggerFactory.getLogger(ScriptUtilities.class);
        inputs = ScriptUtilities.fillDefaultValues(script.getClass(), inputs);
        logger.info("Run script: {} with inputs \n{}", script.getClass().getSimpleName(),
                ((Map<String, Object>) inputs).entrySet().stream().
                        map(entry -> entry.getKey() + "=" + entry.getValue()).
                        collect(java.util.stream.Collectors.joining(", \n")));

        // Check expected arguments
        List<MetaMethod> methods = script.getMetaClass().getMethods();
        MetaMethod execMetaMethod = null;
        // Take the exec method with the most arguments, in case there are multiple exec methods,
        // we want to use the one with the most arguments to provide more features to the script author
        for (MetaMethod method : methods) {
            if (method.getName().equals("exec")) {
                if (execMetaMethod == null || method.getNativeParameterTypes().length > execMetaMethod.getNativeParameterTypes().length) {
                    execMetaMethod = method;
                }
            }
        }
        boolean useProgressVisitor = false; // third argument is a ProgressVisitor
        if (execMetaMethod != null) {
            // 2. Access the native parameter types
            Class[] parameterTypes = execMetaMethod.getNativeParameterTypes();
            if (parameterTypes.length >= 3 && parameterTypes[2].equals(ProgressVisitor.class)) {
                useProgressVisitor = true;
            }
        }
        if(useProgressVisitor) {
            return script.invokeMethod("exec", new Object[] { connection, inputs,
                     progress});
        }
        return script.invokeMethod("exec", new Object[] {connection, inputs});
    }
    /**
     * Read class metadata and fill with default values specified in the script
     * @param groovyClass Groovy class to extract metadata
     * @param inputs Parameters of the exec method
     * @return Filled inputs
     */
    public static Map fillDefaultValues(Class groovyClass, Map inputs) {
        Map filledInputs = new HashMap(inputs);
        ConfigObject config = new ConfigSlurper().parse(groovyClass);
        ((Map<String, Map>) config.get("inputs")).entrySet( ).stream().filter(
                        entry -> entry.getValue().containsKey("default")
                                && !inputs.containsKey(entry.getKey()))
                .forEach(entry -> {
                    Object defaultValue = entry.getValue().get("default");
                    Class<?> expectedType = (Class<?>) entry.getValue().get("type");
                    // Groovy may generate BigDecimal instead of expected class
                    // So cast/convert to the expected type
                    if(expectedType != null && !expectedType.isAssignableFrom(defaultValue.getClass())) {
                        try {
                            defaultValue = ScriptMetadata.castInputUsingExpectedInputType(expectedType, defaultValue.toString());
                        } catch (Exception ex) {
                            Logger logger = LoggerFactory.getLogger(ExecutionPlan.class);
                            logger.info("Warning, failed to cast default value for input '{}', use the original value. Exception: {}",
                                    entry.getKey(), ex.getMessage());
                        }
                    }
                    filledInputs.put(entry.getKey(), defaultValue);
                });
        return filledInputs;
    }
}
