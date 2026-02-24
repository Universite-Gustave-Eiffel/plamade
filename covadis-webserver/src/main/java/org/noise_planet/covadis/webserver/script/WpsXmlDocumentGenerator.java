package org.noise_planet.covadis.webserver.script;

import net.opengis.ows11.*;
import net.opengis.wps10.*;
import org.geotools.wps.WPSConfiguration;
import org.geotools.xsd.Encoder;
import org.locationtech.jts.geom.Geometry;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for generating WPS (Web Processing Service) XML documents.
 * This class provides methods to construct a DescribeProcess XML for a specific script by
 * defining its inputs, outputs, and other metadata.
 */
public class WpsXmlDocumentGenerator {

    private final static Wps10Factory wpsf = Wps10Factory.eINSTANCE;
    private final static Ows11Factory owsf = Ows11Factory.eINSTANCE;
    private final static Map<Class<?>, String> javaClassToXsdType;
    static {
        javaClassToXsdType = new HashMap<>();
        javaClassToXsdType.put(String.class, "xs:string");
        javaClassToXsdType.put(Boolean.class, "xs:boolean");
        javaClassToXsdType.put(Integer.class, "xs:int");
        javaClassToXsdType.put(Double.class, "xs:double");
    }

    /**
     * Shortcut method to generate wps xml generator string
     * @param value String
     * @return Instance
     */
    private static LanguageStringType languageString(String value) {
        LanguageStringType languageStringType = owsf.createLanguageStringType();
        languageStringType.setValue(value);
        return languageStringType;
    }

    /**
     * Parameter identifier
     * @param value String
     * @return Instance
     */
    private static CodeType codetype(String value) {
        CodeType codeType = owsf.createCodeType();
        codeType.setValue(value);
        return codeType;
    }

    /**
     * Creates and initializes a {@link DomainMetadataType} instance with the specified name.
     * It can be a parameter type (String, number.)
     * @param name String
     * @return Instance
     */
    private static DomainMetadataType domainMetadataType(String name) {
        DomainMetadataType domainMetadataType = owsf.createDomainMetadataType();
        domainMetadataType.setValue(name);
        return domainMetadataType;
    }

    public static ValueType valueType(String value) {
        ValueType valueType = owsf.createValueType();
        valueType.setValue(value);
        return valueType;
    }

    public static void dataInputs(DataInputsType inputs, ScriptInput scriptInput) {
        InputDescriptionType input = wpsf.createInputDescriptionType();
        inputs.getInput().add(input);
        input.setIdentifier(codetype(scriptInput.id));
        input.setTitle(languageString(scriptInput.title));
        input.setAbstract(languageString(scriptInput.description));
        input.setMaxOccurs(scriptInput.maxOccurs < 0 ? BigInteger.valueOf(Long.MAX_VALUE) : BigInteger.valueOf(scriptInput.maxOccurs));
        input.setMinOccurs(BigInteger.valueOf(scriptInput.minOccurs));
        LiteralInputType literalInputType = wpsf.createLiteralInputType();
        input.setLiteralData(literalInputType);
        if (scriptInput.type.equals(Boolean.class)) {
            literalInputType.setDataType(domainMetadataType("xs:boolean"));
            literalInputType.setAllowedValues(owsf.createAllowedValuesType());
            literalInputType.getAllowedValues().getValue().add(valueType("true"));
            literalInputType.getAllowedValues().getValue().add(valueType("false"));
        } else {
            literalInputType.setDataType(domainMetadataType(javaClassToXsdType.getOrDefault(scriptInput.type, "xs:string")));
        }
    }

    public static void processOutputs(ProcessOutputsType outputs, ScriptOutput scriptOutput) {
        OutputDescriptionType output = wpsf.createOutputDescriptionType();
        outputs.getOutput().add(output);
        output.setIdentifier(codetype(scriptOutput.id));
        output.setTitle(languageString(scriptOutput.title));
        output.setAbstract(languageString(scriptOutput.description));
        if(!scriptOutput.type.equals(Geometry.class)) {
            output.setLiteralOutput(wpsf.createLiteralOutputType());
            if(scriptOutput.type.equals(Boolean.class)) {
                output.getLiteralOutput().setDataType(domainMetadataType("xs:boolean"));
            } else {
                output.getLiteralOutput().setDataType(domainMetadataType(javaClassToXsdType.getOrDefault(scriptOutput.type, "xs:string")));
            }
        } else {
            // Geometry output is converted to WKT
            SupportedComplexDataType complex = wpsf.createSupportedComplexDataType();
            output.setComplexOutput(complex);
            complex.setSupported(wpsf.createComplexDataCombinationsType());
            ComplexDataDescriptionType ddt = wpsf.createComplexDataDescriptionType();
            ddt.setMimeType("application/wkt");
            complex.getSupported().getFormat().add(ddt);
            ComplexDataDescriptionType def = wpsf.createComplexDataDescriptionType();
            def.setMimeType(ddt.getMimeType());
            complex.setDefault(wpsf.createComplexDataCombinationType());
            complex.getDefault().setFormat(def);
        }
    }

    /**
     * Generates a WPS DescribeProcess XML for a specific Groovy script.
     *
     * @param wrapper the ScriptWrapper representing the script
     * @return XML string for WPS DescribeProcess
     */
    public static String generateDescribeProcessXML(ScriptMetadata wrapper) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ProcessDescriptionsType processDescriptionsType = wpsf.createProcessDescriptionsType();
        processDescriptionsType.setLang("en");
        processDescriptionsType.setService("WPS");

        ProcessDescriptionType processDescriptionType = wpsf.createProcessDescriptionType();
        processDescriptionsType.getProcessDescription().add(processDescriptionType);
        processDescriptionType.setIdentifier(codetype(wrapper.id));
        processDescriptionType.setTitle(languageString(wrapper.title));
        processDescriptionType.setAbstract(languageString(wrapper.description));
        processDescriptionType.setProcessVersion("1.0.0");
        processDescriptionType.setStoreSupported(true);
        processDescriptionType.setStatusSupported(true);
        DataInputsType dataInputsType = wpsf.createDataInputsType();
        processDescriptionType.setDataInputs(dataInputsType);
        for (ScriptInput input : wrapper.inputs.values()) {
            dataInputs(dataInputsType, input);
        }
        processDescriptionType.setProcessOutputs(wpsf.createProcessOutputsType());
        for(ScriptOutput output : wrapper.outputs.values()) {
            processOutputs(processDescriptionType.getProcessOutputs(), output);
        }


        Encoder encoder = new Encoder(new WPSConfiguration());
        encoder.encode(processDescriptionsType, new QName("http://www.opengis.net/wps/1.0.0", "ProcessDescriptions"), baos);
        return baos.toString();
    }



    /**
     * Generates a WPS GetCapabilities XML document listing all available scripts.
     *
     * @param scripts the list of available ScriptWrapper instances
     * @return XML string for WPS GetCapabilities
     */
    public static String generateCapabilitiesXML(List<ScriptMetadata> scripts) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WPSCapabilitiesType capabilities = wpsf.createWPSCapabilitiesType();
        capabilities.setService("WPS");
        capabilities.setVersion("1.0.0");

        ServiceIdentificationType serviceIdentification = owsf.createServiceIdentificationType();
        capabilities.setServiceIdentification(serviceIdentification);
        serviceIdentification.getTitle().add(languageString("NoiseModelling WPS"));
        serviceIdentification.getAbstract().add(languageString("WPS service of NoiseModelling"));

        CodeType serviceType = codetype("WPS");
        serviceIdentification.setServiceType(serviceType);
        serviceIdentification.setServiceTypeVersion ("1.0.0");

        ProcessOfferingsType processOfferings = wpsf.createProcessOfferingsType();
        capabilities.setProcessOfferings(processOfferings);

        for (ScriptMetadata script : scripts) {
            ProcessBriefType process = wpsf.createProcessBriefType();
            process.setProcessVersion("1.0.0");
            process.setIdentifier(codetype(script.id));
            process.setTitle(languageString(script.title));
            process.setAbstract(languageString(script.description));
            processOfferings.getProcess().add(process);
        }

        Encoder encoder = new Encoder(new WPSConfiguration());
        encoder.encode(capabilities, new QName("http://www.opengis.net/wps/1.0.0", "Capabilities"), baos);
        return baos.toString();
    }
}
