import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import utils.RoadValue
import java.nio.file.Paths

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class TestImportGeoClimateData {

    private final Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    private final String location = "Urbach"
    private final String outputDirectory = Paths.get(System.getProperty("user.dir"),"src", "test", "groovy", "outputTest").toString()
    private final Integer srid = 2154
    private final Boolean geoclimatedb = true
    private Map<String, Serializable> params

    /**
     * Create params for geoClimate before each test
     */
    @BeforeEach
    void setup() {
        // Set up parameters before each test
        params = Import_GeoClimate_Data.createGeoClimateConfig(location, outputDirectory, srid, geoclimatedb, logger)
    }

    /**
     * Test if the params for geoClimate input is the good
     */
    @Test
    void testInitialisationParameters() {
        // Test initialization of parameters
        params.forEach((key, value) -> {
            switch (key) {
                case "description":
                    assertEquals("Run the Geoclimate chain and export result to a folder", value)
                    break
                case "geoclimatedb":
                    Map<String, Object> geoclimatedbConfig = (Map<String, Object>) value
                    assertEquals(outputDirectory, geoclimatedbConfig.get("folder"))
                    assertTrue(((String) geoclimatedbConfig.get("name")).startsWith("osm_geoclimate_"))
                    assertEquals(geoclimatedb, geoclimatedbConfig.get("delete"))
                    break
                case "input":
                    Map<String, Object> inputConfig = (Map<String, Object>) value
                    assertEquals(location, ((List<String>) inputConfig.get("locations")).get(0))
                    assertTrue((Boolean) inputConfig.get("delete"))
                    break
                case "output":
                    Map<String, Object> outputConfig = (Map<String, Object>) value
                    assertEquals(srid, outputConfig.get("srid"))
                    Map<String, Object> folderConfig = (Map<String, Object>) outputConfig.get("folder")
                    assertEquals(outputDirectory, folderConfig.get("path").toString())
                    List<String> expectedTables = Arrays.asList("building", "road_traffic", "ground_acoustic", "rail", "zone")
                    assertEquals(expectedTables, folderConfig.get("tables"))
                    break
                case "parameters":
                    Map<String, Object> parametersConfig = (Map<String, Object>) value
                    Map<String, Object> rsuIndicators = (Map<String, Object>) parametersConfig.get("rsu_indicators")
                    assertEquals(Arrays.asList("LCZ"), rsuIndicators.get("indicatorUse"))
                    assertTrue((Boolean) rsuIndicators.get("estimateHeight"))
                    assertTrue((Boolean) parametersConfig.get("worldpop_indicators"))
                    assertTrue((Boolean) parametersConfig.get("road_traffic"))
                    Map<String, Object> noiseIndicators = (Map<String, Object>) parametersConfig.get("noise_indicators")
                    assertTrue((Boolean) noiseIndicators.get("ground_acoustic"))
                    break
                default:
                    throw new IllegalArgumentException("Unexpected config key: " + key)
            }
        })
    }

    /**
     * Test if files are created after call geoClimate with params (without the saving of .mv file)
     */
    @Test
    void testRunGeoClimateWithoutDB() {

        // Test running GeoClimate without saving the database
        Import_GeoClimate_Data.runGeoClimate(params as LinkedHashMap<String, Serializable>, logger)

        // Directory name where files are supposed to exist
        File outDir = Paths.get(outputDirectory, "osm_" + location).toFile()

        // Check if directory exists
        assertEquals(outputDirectory, outDir.getParentFile().toString())

        // Check if there are no files starting with "osm_geoclimate_" and ending with ".mv" in the parent directory
        File parentDir = outDir.getParentFile()
        File[] matchingFiles = parentDir.listFiles(file -> file.getName().startsWith("osm_geoclimate_") && file.getName().endsWith(".mv"))
        assertTrue(matchingFiles == null || matchingFiles.length == 0, "There are osm_geoclimate_*.mv files in the parent directory.")

        List<String> expectedFiles = List.of("building.geojson", "zone.geojson", "ground_acoustic.geojson", "rail.geojson", "road_traffic.geojson")

        // Check if the directory exists
        assertTrue(outDir.exists(), "Chosen zone directory does not exist.")

        // Check the existence of each file
        for (String fileName : expectedFiles) {
            File filePath = Paths.get(outDir.getAbsolutePath(), fileName).toFile()
            assertTrue(filePath.exists(), "File " + fileName + " does not exist.")
        }

        deleteDirectoryRecursively(new File(outputDirectory))
    }

    /**
     * Test if files are created after call geoClimate with params (with the saving of .mv file)
     */
    @Test
    void testRunGeoClimateWithDB() {

        // Test running GeoClimate with saving the database
        Import_GeoClimate_Data.runGeoClimate(Import_GeoClimate_Data.createGeoClimateConfig(location, outputDirectory, srid, false, logger), logger)

        // Directory name where files are supposed to exist
        File outDir = Paths.get(outputDirectory, "osm_" + location).toFile()

        // Check if directory exists
        assertEquals(outputDirectory, outDir.getParentFile().toString())

        // Check if there are no files starting with "osm_geoclimate_" and ending with ".mv" in the parent directory
        File parentDir = outDir.getParentFile()
        File[] matchingFiles = parentDir.listFiles(file -> file.getName().startsWith("osm_geoclimate_") && file.getName().endsWith(".mv"))
        assertTrue(matchingFiles != null || matchingFiles.length == 1, "There are no osm_geoclimate_*.mv files in the parent directory.")

        List<String> expectedFiles = List.of("building.geojson", "zone.geojson", "ground_acoustic.geojson", "rail.geojson", "road_traffic.geojson")

        // Check if the directory exists
        assertTrue(outDir.exists(), "Chosen zone directory does not exist.")

        // Check the existence of each file
        for (String fileName : expectedFiles) {
            File filePath = new File(outDir.getAbsolutePath() + "\\" + fileName)
            assertTrue(filePath.exists(), "File " + fileName + " does not exist.")
        }
        deleteDirectoryRecursively(new File(outputDirectory))
    }

    @Nested
    class TestParseofFileData {

        public static TestImportGeoClimateData testGeoClimateTools = new TestImportGeoClimateData()

        /**
         * Run one time geoClimate before all the test in the class
         */
        @BeforeAll
        static void setupOnce() {
            // Set up once before nested tests
            testGeoClimateTools.setup()
            Import_GeoClimate_Data.runGeoClimate(testGeoClimateTools.params as LinkedHashMap<String, Serializable>, testGeoClimateTools.logger)
        }

        /**
         * Test if the file road_traffic.geojson is correctly parse
         */
        @Test
        void testParseRoad() {
            // Parsing road_traffic data file
            Import_GeoClimate_Data.parseRoadData(outputDirectory, location)

            JsonSlurper jsonSlurper = new JsonSlurper()
            def jsonData = jsonSlurper.parse(Paths.get(outputDirectory, "osm_" + location, "road_traffic.geojson").toFile())

            jsonData.features.each { feature ->

                Map propertiesData = feature.properties

                assertTrue(propertiesData.containsKey(RoadValue.ID_ROAD.nmProperty), "The key 'PK' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.ID_SOURCE.nmProperty), "The key 'ID_SOURCE' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.ROAD_TYPE.nmProperty), "The key 'ROAD_TYPE' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.SURFACE.nmProperty), "The key 'SURFACE' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.SOURCE_ROAD_TYPE.nmProperty), "The key 'SOURCE_ROAD_TYPE' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.SLOPE.nmProperty), "The key 'SLOPE' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.DAY_LV_HOUR.nmProperty), "The key 'LV_D' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.EV_LV_HOUR.nmProperty), "The key 'LV_E' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.NIGHT_LV_HOUR.nmProperty), "The key 'LV_N' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.DAY_LV_SPEED.nmProperty), "The key 'LV_SPD_D' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.EV_LV_SPEED.nmProperty), "The key 'LV_SPD_E' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.NIGHT_LV_SPEED.nmProperty), "The key 'LV_SPD_N' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.DAY_HV_HOUR.nmProperty), "The key 'HGV_D' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.EV_HV_HOUR.nmProperty), "The key 'HGV_E' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.NIGHT_HV_HOUR.nmProperty), "The key 'HGV_N' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.DAY_HV_SPEED.nmProperty), "The key 'HGV_SPD_D' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.EV_HV_SPEED.nmProperty), "The key 'HGV_SPD_E' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.NIGHT_HV_SPEED.nmProperty), "The key 'HGV_SPD_N' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.PAVEMENT.nmProperty), "The key 'PVMT' does not exist in properties of this file")
                assertTrue(propertiesData.containsKey(RoadValue.DIRECTION.nmProperty), "The key 'WAY' does not exist in properties of this file")

                def coordinatesData = feature.geometry.get("coordinates")

                coordinatesData.collect { coordinates ->
                    assertTrue((coordinates as ArrayList).get(2) == 0.5)
                }

            }

        }

        /**
         * Test if the file building.geojson is correctly parse
         */
        @Test
        void testParseBuilding() {
            // Parsing building file
            Import_GeoClimate_Data.parseBuildingData(outputDirectory, location)

            JsonSlurper jsonSlurper = new JsonSlurper()
            def jsonData = jsonSlurper.parse(Paths.get(outputDirectory, "osm_" + location, "building.geojson").toFile())

            jsonData.features.each { feature ->

                Map propertiesData = feature.properties
                assertTrue(propertiesData.containsKey("HEIGHT"), "The key 'HEIGHT' does not exist in properties of this file")

            }
        }

        /**
         * Test if the file dem.geojson is correctly parse
         */
        @Test
        void testParseDEM() {

            JsonSlurper jsonSlurper2 = new JsonSlurper()
            def jsonData2 = jsonSlurper2.parse(Paths.get(outputDirectory, "osm_" + location, "zone.geojson").toFile())

            LinkedHashMap coordinates = [:]

            Integer iterationNumber
            iterationNumber = 1

            jsonData2.features.each { feature ->

                def propertiesData = feature.geometry.coordinates

                propertiesData.each { key ->
                    key.collect { coordinate ->
                        coordinates.put(iterationNumber,coordinate)
                        iterationNumber++
                    }
                }
            }

            iterationNumber = 1

            // Parsing DEM file
            Import_GeoClimate_Data.parseDemData(outputDirectory, location)

            JsonSlurper jsonSlurper1 = new JsonSlurper()
            def jsonData1 = jsonSlurper1.parse(Paths.get(outputDirectory, "osm_" + location, "dem.geojson").toFile())

            jsonData1.features.each { Map feature ->

                // Check if the object contains the key "type" with the value "Feature"
                assertTrue(feature.containsKey("type"), "The key 'type' is missing in the object.")
                assertEquals("Feature", feature.get("type"), "The value of key 'type' is not 'Feature'.")

                // Check if the object contains the key "geometry" with a non-null value
                assertTrue(feature.containsKey("geometry"), "The key 'geometry' is missing in the object.")
                def geometry = feature.get("geometry") as Map

                assertTrue(geometry.containsKey("type"), "The key 'type' is missing in the 'geometry' object.")
                assertEquals("Point", geometry.get("type"), "The value of key 'type' in 'geometry' is not 'Point'.")

                assertTrue(geometry.containsKey("coordinates"), "The key 'coordinates' is missing in the 'geometry' object.")
                assertEquals(2, (geometry.get("coordinates") as List).size(), "The number of values of key 'coordinates' in 'geometry' is not '2'.")
                assertEquals(coordinates.get(iterationNumber),geometry.get("coordinates"), "The coordinates value is note the same.")

                // Check if the object contains the key "properties" with a non-null value
                assertTrue(feature.containsKey("properties"), "The key 'properties' is missing in the object.")
                def properties = feature.get("properties") as Map

                assertTrue(properties.containsKey("height"), "The key 'height' is missing in the 'properties' object.")
                assertEquals(0.0, properties.get("height"), "The value of key 'height' in 'properties' is not '0.0'.")

                iterationNumber++
            }

        }

        /**
         * Delete outputTest folder after all test are running
         */
        @AfterAll
        static void resetOnce() {
            // Clean up once after nested tests
            testGeoClimateTools.deleteDirectoryRecursively(new File(testGeoClimateTools.outputDirectory))
        }
    }

    /**
     * Recursively deletes the outPutDir create for test
     * @param dir path of the directory
     */
    protected void deleteDirectoryRecursively(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles()
            if (children != null) {
                for (File child : children) {
                    deleteDirectoryRecursively(child)
                }
            }
        }
        dir.delete()
    }
}