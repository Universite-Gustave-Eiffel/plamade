package org.noise_planet.covadis.webserver;

import net.opengis.wps10.ExecuteType;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.noise_planet.covadis.webserver.script.WpsXmlDocumentGenerator;
import org.noise_planet.covadis.webserver.script.ScriptMetadata;
import org.noise_planet.covadis.webserver.script.WpsScriptWrapper;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class TestParseWPSQueries {

    @Test
    public void testDelaunayParse() throws IOException, ParserConfigurationException, SAXException {

        // Build ScriptWrapper
        List<ScriptMetadata> wrappers = WpsScriptWrapper.buildScriptWrappers(WpsScriptWrapper.scanScriptsGrouped(ClassLoader.getSystemClassLoader(),
                Path.of("scripts")));
        assertNotEquals(0, wrappers.size());
        // look for the script named Delaunay_Grid
        Optional<ScriptMetadata> scriptMetadata = wrappers.stream().filter(sw -> sw.id.equals("Receivers:Delaunay_Grid"))
                .findFirst();
        assertTrue(scriptMetadata.isPresent());
        assertEquals("Receivers:Delaunay_Grid", scriptMetadata.get().id);

        String requestBody = "<p0:Execute xmlns:p0=\"http://www.opengis.net/wps/1.0.0\" service=\"WPS\" version=\"1.0" +
                ".0\"><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">Receivers:Delaunay_Grid</p1:Identifier><p0:DataInputs><p0:Input><p1:Identifier " +
                "xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">tableBuilding</p1:Identifier><p0:Data><p0:LiteralData>BUILDINGS_LOW_HEIGHT</p0:LiteralData></p0:Data></p0" +
                ":Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">sourcesTableName</p1:Identifier><p0:Data><p0:LiteralData>ROADS</p0:LiteralData></p0:Data></p0" +
                ":Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">exportTrianglesGeometries</p1:Identifier><p0:Data><p0:LiteralData>true</p0:LiteralData></p0" +
                ":Data></p0:Input><p0:Input><p1:Identifier xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">isoSurfaceInBuildings</p1:Identifier><p0:Data><p0:LiteralData>false</p0:LiteralData></p0" +
                ":Data></p0:Input></p0:DataInputs><p0:ResponseForm><p0:RawDataOutput><p1:Identifier " +
                "xmlns:p1=\"http://www.opengis.net/ows/1" +
                ".1\">result</p1:Identifier></p0:RawDataOutput></p0:ResponseForm></p0:Execute>";

        // Provide request body as an input stream
        ExecuteType executeType = OwsController.parseExecuteRequest(new ByteArrayInputStream(requestBody.getBytes()));
        assertNotNull(executeType);
        Map<String, Object> inputs = scriptMetadata.get().extractInputs(executeType);
        // Check inputs values and type
        assertEquals("BUILDINGS_LOW_HEIGHT", inputs.get("tableBuilding"));
        assertEquals(Boolean.class, inputs.get("exportTrianglesGeometries").getClass());
        assertEquals(true, inputs.get("exportTrianglesGeometries"));
        assertEquals(Boolean.class, inputs.get("isoSurfaceInBuildings").getClass());
        assertEquals(false, inputs.get("isoSurfaceInBuildings"));

    }


    @Test
    public void testGeometryReturnParse() throws IOException, ParserConfigurationException, SAXException {

        // Build ScriptWrapper
        List<ScriptMetadata> wrappers = WpsScriptWrapper.buildScriptWrappers(WpsScriptWrapper.scanScriptsGrouped(ClassLoader.getSystemClassLoader(), Path.of("scripts")));
        assertNotEquals(0, wrappers.size());
        // look for the script named Delaunay_Grid
        Optional<ScriptMetadata> scriptMetadata = wrappers.stream().filter(sw -> sw.id.equals("Database_Manager:Table_Visualization_Map")).findFirst();
        assertTrue(scriptMetadata.isPresent());
        assertEquals("Database_Manager:Table_Visualization_Map", scriptMetadata.get().id);

        assertTrue(scriptMetadata.get().outputs.containsKey("result"));
        assertEquals(Geometry.class, scriptMetadata.get().outputs.get("result").type);

        String describeProcessXML = WpsXmlDocumentGenerator.generateDescribeProcessXML(scriptMetadata.get());

        // Expect XML output with WKT Geometry type
        assertTrue(describeProcessXML.contains("application/wkt"));
        assertTrue(describeProcessXML.contains("<ows:Identifier>Database_Manager:Table_Visualization_Map</ows:Identifier>"));
    }

    @Test
    public void testGenerateCapabilitiesXML() throws IOException {
        // Build ScriptWrapper
        List<ScriptMetadata> wrappers =
                WpsScriptWrapper.buildScriptWrappers(WpsScriptWrapper.scanScriptsGrouped(ClassLoader.getSystemClassLoader(), Path.of("scripts")));
        assertNotEquals(0, wrappers.size());

        String capabilitiesXML = WpsXmlDocumentGenerator.generateCapabilitiesXML(wrappers);

        // Check that capabilities XML is not empty
        assertNotNull(capabilitiesXML);
        assertFalse(capabilitiesXML.isEmpty());

        // Expect XML to contain WPS capabilities elements
        assertTrue(capabilitiesXML.contains("Capabilities"));
        assertTrue(capabilitiesXML.contains("ProcessOfferings"));
        assertTrue(capabilitiesXML.contains("ows:Identifier"));
        assertTrue(capabilitiesXML.contains("Receivers:Building_Grid"));
    }


}
