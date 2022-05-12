package org.noise_planet.plamade.process;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.apache.commons.text.StringSubstitutor;
import org.cts.CRSFactory;
import org.cts.IllegalCoordinateException;
import org.cts.crs.CRSException;
import org.cts.crs.CoordinateReferenceSystem;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationException;
import org.cts.op.CoordinateOperationFactory;
import org.cts.registry.EPSGRegistry;
import org.cts.registry.RegistryManager;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.functions.io.shp.SHPWrite;
import org.h2gis.functions.spatial.crs.SpatialRefRegistry;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.math.Vector2D;
import org.noise_planet.nmcluster.NoiseModellingInstance;
import org.noise_planet.noisemodelling.jdbc.LDENComputeRaysOut;
import org.noise_planet.noisemodelling.jdbc.LDENConfig;
import org.noise_planet.noisemodelling.jdbc.LDENPointNoiseMapFactory;
import org.noise_planet.noisemodelling.jdbc.LDENPropagationProcessData;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.CnossosPropagationData;
import org.noise_planet.noisemodelling.pathfinder.ComputeCnossosRays;
import org.noise_planet.noisemodelling.pathfinder.IComputeRaysOut;
import org.noise_planet.noisemodelling.pathfinder.PointPath;
import org.noise_planet.noisemodelling.pathfinder.ProfileBuilder;
import org.noise_planet.noisemodelling.pathfinder.PropagationPath;
import org.noise_planet.noisemodelling.pathfinder.QueryRTree;
import org.noise_planet.noisemodelling.pathfinder.utils.PowerUtils;
import org.noise_planet.noisemodelling.propagation.ComputeRaysOutAttenuation;
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class NoiseModellingProfileReport {
    CRSFactory crsf = new CRSFactory();
    SpatialRefRegistry srr = new SpatialRefRegistry();
    private CoordinateOperation transform = null;
    int sridBuildings;

    public void setInputCRS(String crs) throws CRSException, CoordinateOperationException {
        // Create a new CRSFactory, a necessary element to create a CRS without defining one by one all its components
        CRSFactory cRSFactory = new CRSFactory();

        // Add the appropriate registry to the CRSFactory's registry manager. Here the EPSG registry is used.
        RegistryManager registryManager = cRSFactory.getRegistryManager();
        registryManager.addRegistry(new EPSGRegistry());

        // CTS will read the EPSG registry seeking the 4326 code, when it finds it,
        // it will create a CoordinateReferenceSystem using the parameters found in the registry.
        CoordinateReferenceSystem crsGeoJSON = cRSFactory.getCRS("EPSG:4326");
        CoordinateReferenceSystem crsSource = cRSFactory.getCRS(crs);
        if(crsGeoJSON instanceof GeodeticCRS && crsSource instanceof GeodeticCRS) {
            transform = CoordinateOperationFactory.createCoordinateOperations((GeodeticCRS) crsSource, (GeodeticCRS) crsGeoJSON).iterator().next();
        }
    }

    public static Double asDouble(Object v) {
        if(v instanceof Number) {
            return ((Number)v).doubleValue();
        } else {
            return null;
        }
    }

    public static Integer asInteger(Object v) {
        if(v instanceof Number) {
            return ((Number)v).intValue();
        } else {
            return null;
        }
    }

    public void generateLeafletMap(int id, StringBuilder sb, List<ProfileBuilder.CutPoint> cutPoints, Geometry sourceGeometry) throws IllegalCoordinateException, CoordinateOperationException, CRSException, URISyntaxException, IOException {
        StringBuilder propagationLines = new StringBuilder("L.polyline([");
        Envelope envelope = new Envelope();
        String mapBoxAccessToken = "pk.eyJ1IjoibWFwYm94IiwiYSI6ImNpejY4NXVycTA2emYycXBndHRqcmZ3N3gifQ.rJcFIG214AriISLbB6B5aw";
        int pointIndex = 0;
        for (ProfileBuilder.CutPoint cutPoint : cutPoints) {
            Coordinate c = cutPoint.getCoordinate();
            envelope.expandToInclude(c);
            Coordinate wgs84Coordinate = reproject(c, sridBuildings, 4326);
            if(pointIndex > 0) {
                propagationLines.append(",");
            }
            propagationLines.append(String.format(Locale.ROOT, "[%.8f, %.8f, %.8f]", wgs84Coordinate.y,
                    wgs84Coordinate.x, Double.isNaN(c.z) ? 0 : c.z));
            pointIndex++;
        }
        propagationLines.append("], {color: 'red'}).bindPopup(\"").append("Ray n°").append(id).append("\").addTo(map").append(id).append(");\n");
        // add source geom
        Coordinate[] sourceCoordinates = sourceGeometry.getCoordinates();
        if(sourceCoordinates.length > 1) {
            propagationLines.append("L.polyline([");
            pointIndex = 0;
            for (Coordinate sourceCoordinate : sourceCoordinates) {
                if(pointIndex > 0) {
                    propagationLines.append(",");
                }
                Coordinate wgs84Coordinate = reproject(sourceCoordinate, sridBuildings, 4326);
                propagationLines.append(String.format(Locale.ROOT, "[%.8f, %.8f, %.8f]", wgs84Coordinate.y,
                        wgs84Coordinate.x, Double.isNaN(sourceCoordinate.z) ? 0 : sourceCoordinate.z));
                pointIndex++;
            }
        } else {
            Coordinate wgs84Coordinate = reproject(sourceCoordinates[0], sridBuildings, 4326);
            propagationLines.append(String.format(Locale.ROOT, "L.marker([%f, %f,", wgs84Coordinate.y, wgs84Coordinate.x));
        }
        propagationLines.append("], {color: 'black'}).bindPopup(\"").append("Source").append("\").addTo(map").append(id).append(");\n");
        // reproject to crs
        Coordinate env = reproject(envelope.centre(), sridBuildings, 4326);
        String raysMap = pageToString(Map.of("mapIdentifier", id,
                "viewLatititude", env.y,
                "viewLongitude", env.x,
                "mapBoxToken", mapBoxAccessToken,
                "markers", propagationLines.toString()), "leafletmap.html");
        sb.append(raysMap);
    }

    public static void generateCutPointsVega(int id, StringBuilder sb, List<PointPath> cutPoints) throws URISyntaxException, IOException {
        if(cutPoints.size() < 2) {
            return;
        }
        StringBuilder data = new StringBuilder();
        for (PointPath current : cutPoints) {
            if(data.length() > 0) {
                data.append(",\n");
            }
            data.append("{\"x\": " );
            data.append(String.format(Locale.ROOT, "%.1f", current.coordinate.x));
            data.append(", \"y\": ");
            data.append(String.format(Locale.ROOT, "%.2f", Double.isNaN(current.altitude) || Double.compare(current.altitude, 0) == 0 ? current.coordinate.y : current.altitude));
            data.append(", \"z\": ");
            if(!Double.isNaN(current.buildingHeight) && current.buildingHeight > 0) {
                data.append(String.format(Locale.ROOT, "%.2f", current.coordinate.y + current.buildingHeight));
            } else {
                data.append(String.format(Locale.ROOT, "%.2f", current.coordinate.y));
            }
            data.append(", \"type\": \"");
            data.append(current.type.toString());
            data.append("\"");
            data.append("}");
        }
        sb.append(pageToString(Map.of("data", data.toString(), "graphID", id), "vega_graph.html"));
    }

    public static void pushArray(StringBuilder tables, String title, double[] attenuationTable) {
        tables.append("<tr><th>");
        tables.append(title);
        tables.append("</th>");
        for (double v : attenuationTable) {
            tables.append("<td>");
            tables.append(String.format(Locale.ROOT, "%.2f", v));
            tables.append("</td>");
        }
        tables.append("</tr>");
    }

    public void exportHtml(CnossosPropagationData propagationData,
                           LDENPointNoiseMapFactory ldenPointNoiseMapFactory,
                           LDENComputeRaysOut ldenPropagationProcessData,  File path) throws IOException,
            URISyntaxException, IllegalCoordinateException, CRSException, CoordinateOperationException {
        Map<Integer, ArrayList<PropagationPath>> propagationPathPerReceiver = new HashMap<>();

        // Merge rays by receiver identifier
        Map<Integer, Integer> sourcePkToLocalIndex = new HashMap<>();
        for(int idSource = 0; idSource < propagationData.sourcesPk.size(); idSource++) {
            sourcePkToLocalIndex.put(propagationData.sourcesPk.get(idSource).intValue(), idSource);
        }
        int rayIdentifier = 0;
        StringBuilder tables = new StringBuilder();
        for(PropagationPath propagationPath : ldenPointNoiseMapFactory.getLdenData().rays) {
            ArrayList<PropagationPath> ar = propagationPathPerReceiver.computeIfAbsent(
                    propagationPath.getIdReceiver(), k1 -> new ArrayList<>());
            ar.add(propagationPath);
            int pointIndex = 0;
            tables.append("<h1>Ray n°");
            tables.append(rayIdentifier);
            tables.append("</h1>");
            Geometry sourceGeometry = propagationData.sourceGeometries.get(
                    propagationData.sourcesPk.indexOf((long)propagationPath.getIdSource()));
            generateLeafletMap(rayIdentifier, tables, propagationPath.getCutPoints(), sourceGeometry);
            generateCutPointsVega(rayIdentifier, tables, propagationPath.getPointList());
            tables.append("<table id=\"attable\"><thead><tr><th>f in Hz</th>");
            for (Integer frequency : propagationData.freq_lvl) {
                tables.append("<th>");
                tables.append(frequency);
                tables.append("</th>");
            }
            tables.append("</tr></thead><tbody>");
            pushArray(tables, "aAtm", propagationPath.absorptionData.aAtm);
            pushArray(tables, "aDiv", propagationPath.absorptionData.aDiv);
            pushArray(tables, "aRef", propagationPath.absorptionData.aRef);
            pushArray(tables, "dLAbs", propagationPath.reflectionAttenuation.dLAbs);
            pushArray(tables, "dLRetro", propagationPath.reflectionAttenuation.dLRetro);
            pushArray(tables, "aBoundaryH", propagationPath.absorptionData.aBoundaryH);
            pushArray(tables, "aBoundaryF", propagationPath.absorptionData.aBoundaryF);
            pushArray(tables, "aGroundF", propagationPath.groundAttenuation.aGroundF);
            pushArray(tables, "aGroundH", propagationPath.groundAttenuation.aGroundH);
            pushArray(tables, "aDifH", propagationPath.absorptionData.aDifH);
            pushArray(tables, "aDifF", propagationPath.absorptionData.aDifF);
            pushArray(tables, "aSource", propagationPath.absorptionData.aSource);
            pushArray(tables, "aGlobalH", propagationPath.absorptionData.aGlobalH);
            pushArray(tables, "aGlobalF", propagationPath.absorptionData.aGlobalF);
            pushArray(tables, "aGlobal", propagationPath.absorptionData.aGlobal);
            tables.append("</tbody></table>");
            rayIdentifier++;
        }

        try(Writer writer = new BufferedWriter(new FileWriter(path))){
            writer.write(pageToString(Map.of("tables", tables), "report_page.html"));
        }
    }

    private static String pageToString(Map<String, Object> parameters, String resourceName) throws URISyntaxException, IOException {
        StringSubstitutor stringSubstitutor = new StringSubstitutor(parameters);
        String content = Files.lines(Paths.get(NoiseModellingProfileReport.class.
                getResource(resourceName).toURI())).collect(Collectors.joining(System.lineSeparator()));
        return stringSubstitutor.replace(content);
    }

    private Coordinate reproject(Coordinate coordinate, int inputCRSCode, int outputCRSCode) throws IllegalCoordinateException, CoordinateOperationException, CRSException {
        CoordinateReferenceSystem inputCRS = crsf.getCRS(srr.getRegistryName() + ":" + inputCRSCode);
        CoordinateReferenceSystem targetCRS = crsf.getCRS(srr.getRegistryName() + ":" + outputCRSCode);
        Set<CoordinateOperation> ops = CoordinateOperationFactory.createCoordinateOperations((GeodeticCRS)inputCRS, (GeodeticCRS)targetCRS);
        if (ops.isEmpty()) {
            return coordinate;
        }
        CoordinateOperation op = CoordinateOperationFactory.getMostPrecise(ops);
        double[] srcPointLocalDoubles = op.transform(new double[] {coordinate.x, coordinate.y});
        return new Coordinate(srcPointLocalDoubles[0], srcPointLocalDoubles[1]);
    }

    public void testDebugNoiseProfile(String workingDir, Coordinate receiverCoordinate, Coordinate sourceCoordinate,
                                      String uueid, NoiseModellingInstance.SOURCE_TYPE sourceType) throws SQLException, IOException,
            IllegalCoordinateException, CoordinateOperationException, CRSException, URISyntaxException {
        // TODO generate JSON for https://github.com/renhongl/json-viewer-js
        Logger logger = LoggerFactory.getLogger("debug");
        DataSource ds = NoiseModellingRunner.createDataSource("", "",
                workingDir, "h2gisdb", false);
        try(Connection connection = new ConnectionWrapper(ds.getConnection())) {
            crsf.getRegistryManager().addRegistry(srr);
            srr.setConnection(connection);

            String sourceTable = "LW_ROADS";
            if(sourceType == NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                sourceTable = "LW_RAILWAY";
            }
            String receiversTable = "RECEIVERS_TEST";
            int configurationId = 4;

            Sql sql = new Sql(connection);

            GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
            int reflectionOrder = asInteger(rs.get("confreflorder"));
            int maxSrcDist = asInteger(rs.get("confmaxsrcdist"));
            int maxReflectionDistance = asInteger(rs.get("confmaxrefldist"));
            double wallAlpha = asDouble(rs.get("wall_alpha"));
            // overwrite with the system number of thread - 1
            Runtime runtime = Runtime.getRuntime();
            int nThread = Math.max(1, runtime.availableProcessors() - 1);

            boolean compute_vertical_edge_diffraction = (Boolean)rs.get("confdiffvertical");
            boolean compute_horizontal_edge_diffraction = (Boolean)rs.get("confdiffhorizontal");

            if(!uueid.isEmpty()) {
                // Keep only receivers near selected UUEID
                String conditionReceiver = "";
                logger.info("Fetch receivers near roads with uueid " + uueid);
                try(Statement st = connection.createStatement()) {
                    sql.execute("DROP TABLE IF EXISTS SOURCES_SPLITED;");
                    if(sourceType == NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                        sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) THE_GEOM FROM RAIL_SECTIONS WHERE UUEID = ''"+uueid+"'')');");
                    } else {
                        sql.execute("CREATE TABLE SOURCES_SPLITED AS SELECT * from ST_EXPLODE('(SELECT ST_ToMultiSegments(THE_GEOM) THE_GEOM FROM ROADS WHERE UUEID = ''"+uueid+"'')');");
                    }
                    logger.info("Fetch receivers near uueid " + uueid);
                    sql.execute("CREATE SPATIAL INDEX ON SOURCES_SPLITED(THE_GEOM)");
                    sql.execute("DROP TABLE IF EXISTS RECEIVERS_UUEID");
                    sql.execute("CREATE TABLE RECEIVERS_UUEID(THE_GEOM geometry, PK integer not null, PK_1 integer, RCV_TYPE integer) AS SELECT R.* FROM RECEIVERS R WHERE "+conditionReceiver+" EXISTS (SELECT 1 FROM SOURCES_SPLITED R2 WHERE ST_EXPAND(R.THE_GEOM, "+maxSrcDist+", "+maxSrcDist+") && R2.THE_GEOM AND ST_DISTANCE(R.THE_GEOM, R2.THE_GEOM) <= "+maxSrcDist+" LIMIT 1)");
                    logger.info("Create primary key and index on filtered receivers");
                    sql.execute("ALTER TABLE RECEIVERS_UUEID ADD PRIMARY KEY(PK)");
                    sql.execute("CREATE INDEX RECEIVERS_UUEID_PK1 ON RECEIVERS_UUEID(PK_1)");
                    sql.execute("CREATE SPATIAL INDEX RECEIVERS_UUEID_SPI ON RECEIVERS_UUEID (THE_GEOM)");
                    // Filter only sound source that match the UUEID
                    logger.info("Fetch sound sources that match with uueid " + uueid);
                    sql.execute("DROP TABLE IF EXISTS LW_UUEID");
                    if(sourceType == NoiseModellingInstance.SOURCE_TYPE.SOURCE_TYPE_RAIL) {
                        sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_RAILWAY LW WHERE UUEID = '" + uueid + "'");
                    } else {
                        sql.execute("CREATE TABLE LW_UUEID AS SELECT LW.* FROM LW_ROADS LW, ROADS R WHERE LW.PK = R.PK AND R.UUEID = '" + uueid + "'");
                    }
                    sql.execute("ALTER TABLE LW_UUEID ALTER COLUMN PK INTEGER NOT NULL");
                    sql.execute("ALTER TABLE LW_UUEID ADD PRIMARY KEY(PK)");
                    sql.execute("CREATE SPATIAL INDEX ON LW_UUEID(THE_GEOM)");
                    int nbSources = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM LW_UUEID").get("CPT"));
                    logger.info(String.format(Locale.ROOT, "There is %d sound sources with this UUEID", nbSources));
                    int nbReceivers = asInteger(sql.firstRow("SELECT COUNT(*) CPT FROM RECEIVERS_UUEID").get("CPT"));
                    logger.info(String.format(Locale.ROOT, "There is %d receivers with this UUEID", nbReceivers));
                    sourceTable = "LW_UUEID";
                    SHPWrite.exportTable(connection, new File(workingDir, "LW_"+uueid+".shp").getAbsolutePath(), sourceTable, "UTF-8", true);
                }
            }

            DBTypes dbTypes = DBUtils.getDBType(connection);
            sridBuildings = GeometryTableUtilities.getSRID(connection, TableLocation.parse("BUILDINGS_SCREENS", dbTypes));

            setInputCRS("EPSG:"+ sridBuildings);

            Coordinate srcPoint = reproject(sourceCoordinate, 4326, sridBuildings);
            Coordinate axeReceiver = reproject(receiverCoordinate, 4326, sridBuildings);
            int distance = 50;
            //int step = 5;
            //double receiverHeight = 4.0;
            double sourceHeight = 0.5;

            Statement st = connection.createStatement();

            st.execute("DROP TABLE IF EXISTS RECEIVERS_TEST");
            st.execute("CREATE TABLE RECEIVERS_TEST(PK serial primary key, the_geom geometry(POINTZ, " + sridBuildings + "))");

            Vector2D vector2D = new Vector2D(srcPoint, axeReceiver).normalize();

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " +
                    "RECEIVERS_TEST(THE_GEOM) VALUES (?)");
            GeometryFactory factory = new GeometryFactory();
            Point geom = factory.createPoint(new Coordinate(axeReceiver.x, axeReceiver.y, receiverCoordinate.z));
            geom.setSRID(sridBuildings);
            preparedStatement.setObject(1, geom);
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
//            for(int i = -distance ; i < distance; i += step) {
//                if(i != 0) {
//                    Coordinate receiverCoordinate = new Coordinate(srcPoint.x +
//                            vector2D.multiply(i).getX(), srcPoint.y +
//                            vector2D.multiply(i).getY(), receiverHeight);
//                    Point geom = factory.createPoint(receiverCoordinate);
//                    geom.setSRID(sridBuildings);
//                    preparedStatement.setObject(1, geom);
//                    preparedStatement.addBatch();
//                    preparedStatement.executeBatch();
//                }
//            }

            PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_SCREENS",
                    sourceTable, receiversTable);


            pointNoiseMap.setComputeVerticalDiffraction(compute_horizontal_edge_diffraction);
            pointNoiseMap.setComputeHorizontalDiffraction(compute_vertical_edge_diffraction);
            pointNoiseMap.setSoundReflectionOrder(reflectionOrder);

            // Set environmental parameters
            GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");
            String[] fieldTemperature = new String[] {"TEMP_D" ,"TEMP_E" ,"TEMP_N"};
            String[] fieldHumidity = new String[] {"HYGRO_D", "HYGRO_E", "HYGRO_N"};
            String[] fieldPFav = new String[] {"PFAV_06_18", "PFAV_18_22", "PFAV_22_06"};

            for(int idTime = 0; idTime < LDENConfig.TIME_PERIOD.values().length; idTime++) {
                PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);
                double confHumidity = asDouble(row_zone.get(fieldHumidity[idTime]));
                double confTemperature = asDouble(row_zone.get(fieldTemperature[idTime]));
                String confFavorableOccurrences = (String) row_zone.get(fieldPFav[idTime]);
                environmentalData.setHumidity(confHumidity);
                environmentalData.setTemperature(confTemperature);
                StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ",");
                double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length];
                for (int i = 0; i < favOccurrences.length; i++) {
                    favOccurrences[i] = Math.max(0, Math.min(1, Double.parseDouble(tk.nextToken().trim())));
                }
                environmentalData.setWindRose(favOccurrences);
                pointNoiseMap.setPropagationProcessPathData(LDENConfig.TIME_PERIOD.values()[idTime], environmentalData);
            }

            LDENConfig ldenConfig = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

            ldenConfig.setComputeLDay(true);
            ldenConfig.setComputeLEvening(true);
            ldenConfig.setComputeLNight(true);

            LDENPointNoiseMapFactory ldenPointNoiseMapFactory = new LDENPointNoiseMapFactory(connection, ldenConfig);
            pointNoiseMap.setPropagationProcessDataFactory(ldenPointNoiseMapFactory);

            // Building height field name
            pointNoiseMap.setHeightField("HEIGHT");
            // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
            pointNoiseMap.setSoilTableName("landcover");
            // Point cloud height above sea level POINT(X Y Z)
            pointNoiseMap.setDemTable("DEM");



            pointNoiseMap.setMaximumPropagationDistance(maxSrcDist);
            pointNoiseMap.setMaximumReflectionDistance(maxReflectionDistance);
            pointNoiseMap.setWallAbsorption(wallAlpha);


            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>();

            // Do not propagate for low emission or far away sources
            // Maximum error in dB
            pointNoiseMap.setMaximumError(0.2d);



            AtomicInteger k = new AtomicInteger();

            pointNoiseMap.setThreadCount(1);

            pointNoiseMap.initialize(connection, new EmptyProgressVisitor());

            pointNoiseMap.setGridDim(1);

            Envelope computationEnvelope = new Envelope(axeReceiver);
            computationEnvelope.expandBy(maxSrcDist);
            pointNoiseMap.setMainEnvelope(computationEnvelope);

            CnossosPropagationData propagationData = pointNoiseMap.prepareCell(connection, 0, 0, new EmptyProgressVisitor(), receivers);

            ComputeCnossosRays computeRays = new ComputeCnossosRays(propagationData);
            computeRays.setThreadCount(1);

            computeRays.makeReceiverRelativeZToAbsolute();

            computeRays.makeSourceRelativeZToAbsolute();

            //Run computation

            LDENComputeRaysOut ldenPropagationProcessData = (LDENComputeRaysOut) ldenPointNoiseMapFactory.create(propagationData,
                    pointNoiseMap.getPropagationProcessPathDataDay(),
                    pointNoiseMap.getPropagationProcessPathDataEvening(),
                    pointNoiseMap.getPropagationProcessPathDataNight());

            ldenPropagationProcessData.keepRays = true;
            ldenPropagationProcessData.keepAbsorption = true;

            computeRays.run(ldenPropagationProcessData);

            exportHtml(propagationData, ldenPointNoiseMapFactory, ldenPropagationProcessData, new File(workingDir, "report.html"));
//
//            Map<Integer, ArrayList<PropagationPath>> propagationPathPerReceiver = new HashMap<>();
//
//            for(PropagationPath propagationPath : ldenPointNoiseMapFactory.getLdenData().rays) {
//                ArrayList<PropagationPath> ar = propagationPathPerReceiver.computeIfAbsent(
//                        propagationPath.getIdReceiver(), k1 -> new ArrayList<>());
//                ar.add(propagationPath);
//                Coordinate receiver = propagationPath.getSRSegment().r;
//                Coordinate source = propagationPath.getSRSegment().s;
//               // logger.info("(Src:"+ propagationPath.getIdSource() + " R:"+propagationPath.getIdReceiver()+") Distance "+   receiver.distance(source) + " m " + Arrays.toString(propagationPath.absorptionData.aGlobal));
//            }
//
//
//            try(FileOutputStream fos = new FileOutputStream(new File(workingDir, "debug.geojson").getAbsoluteFile())){
//                JsonFactory jsonFactory = new JsonFactory();
//                JsonEncoding jsonEncoding = JsonEncoding.UTF8;
//                JsonGenerator jsonGenerator = jsonFactory.createGenerator(new BufferedOutputStream(fos), jsonEncoding);
//                ObjectMapper mapper = new ObjectMapper();
//                mapper.enable(SerializationFeature.INDENT_OUTPUT);
//                mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
//                        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
//                        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
//                        .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
//                        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
//                        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
//                jsonGenerator.setCodec(mapper);
//                // header of the GeoJSON file
//                jsonGenerator.writeStartObject();
//                jsonGenerator.writeStringField("type", "FeatureCollection");
//                jsonGenerator.writeArrayFieldStart("features");
//                for (ComputeRaysOutAttenuation.VerticeSL v : ldenPointNoiseMapFactory.getLdenData().lDayLevels) {
//                    double globalDba = PowerUtils.wToDba(PowerUtils.sumArray(PowerUtils.dbaToW(v.value)));
//                    if(!Double.isNaN(globalDba) && Double.isFinite(globalDba)) {
//                        globalDba = Math.round(globalDba * 100.0) / 100.0;
//                    }
//                    jsonGenerator.writeStartObject();
//                    jsonGenerator.writeStringField("type", "Feature");
//                    int localReceiverIndex = ldenPropagationProcessData.inputData.receiversPk.indexOf(v.receiverId);
//                    Coordinate realReceiverCoordinate = ldenPropagationProcessData.inputData.receivers.get(localReceiverIndex);
//                    writePoint(realReceiverCoordinate, jsonGenerator, transform);
//                    jsonGenerator.writeObjectFieldStart("properties");
//                    jsonGenerator.writeFieldName("level");
//                    jsonGenerator.writeNumber(globalDba);
//                    jsonGenerator.writeFieldName("pk");
//                    jsonGenerator.writeNumber(v.receiverId);
//                    jsonGenerator.writeArrayFieldStart("rays");
//                    List<PropagationPath> rays =  propagationPathPerReceiver.get((int)v.receiverId);
//                    for(PropagationPath propagationPath : rays.subList(0, Math.min(10, rays.size()))) {
//                        mapper.writeValue(jsonGenerator, propagationPath);
//                    }
//                    jsonGenerator.writeEndArray();
//                    jsonGenerator.writeArrayFieldStart("profile");
//                    ProfileBuilder.CutProfile profile = ldenPropagationProcessData.inputData.profileBuilder.getProfile(srcPoint, axeReceiver, 0);
//                    for(ProfileBuilder.CutPoint cutPoint : profile.getCutPoints()) {
//                        if(cutPoint.getType() == ProfileBuilder.IntersectionType.TOPOGRAPHY) {
//                            double zGround = cutPoint.getCoordinate().z;
//                            jsonGenerator.writeNumber(Math.round(zGround * 100) / 100.0);
//                        }
//                    }
//                    jsonGenerator.writeEndArray();
//                    jsonGenerator.writeEndObject();
//                    // feature footer
//                    jsonGenerator.writeEndObject();
//                }
//                // footer
//                jsonGenerator.writeEndArray();
//                jsonGenerator.writeEndObject();
//                jsonGenerator.flush();
//                jsonGenerator.close();
//            }
        }
    }


    public static void writePoint(Coordinate coordinate, JsonGenerator gen, CoordinateOperation coordinateOperation) throws IOException {
        gen.writeObjectFieldStart("geometry");
        gen.writeStringField("type", "Point");
        gen.writeFieldName("coordinates");
        writeCoordinate(coordinate, gen, coordinateOperation);
        gen.writeEndObject();
    }

    public static void writeCoordinate(Coordinate coordinate, JsonGenerator gen, CoordinateOperation coordinateOperation) throws IOException {
        gen.writeStartArray();
        if(coordinateOperation != null) {
            try {
                double[] xyz = coordinateOperation.transform(new double[]{coordinate.x, coordinate.y, coordinate.z});
                coordinate = new Coordinate(xyz[0], xyz[1], coordinate.z);
            } catch (CoordinateOperationException | IllegalCoordinateException ex) {
                throw new IOException("Error wile re-project", ex);
            }
        }
        gen.writeNumber(coordinate.x);
        gen.writeNumber(coordinate.y);
        if (!Double.isNaN(coordinate.getZ())) {
            gen.writeNumber(coordinate.getZ());
        }
        gen.writeEndArray();
    }
}
