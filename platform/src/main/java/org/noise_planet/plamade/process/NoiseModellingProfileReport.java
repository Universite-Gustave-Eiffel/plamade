package org.noise_planet.plamade.process;

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.cts.CRSFactory;
import org.cts.IllegalCoordinateException;
import org.cts.crs.CRSException;
import org.cts.crs.CoordinateReferenceSystem;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationException;
import org.cts.op.CoordinateOperationFactory;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.functions.spatial.crs.SpatialRefRegistry;
import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.math.Vector2D;
import org.noise_planet.noisemodelling.jdbc.LDENConfig;
import org.noise_planet.noisemodelling.jdbc.LDENPointNoiseMapFactory;
import org.noise_planet.noisemodelling.jdbc.PointNoiseMap;
import org.noise_planet.noisemodelling.pathfinder.CnossosPropagationData;
import org.noise_planet.noisemodelling.pathfinder.ComputeCnossosRays;
import org.noise_planet.noisemodelling.pathfinder.ProfileBuilder;
import org.noise_planet.noisemodelling.pathfinder.PropagationPath;
import org.noise_planet.noisemodelling.pathfinder.QueryRTree;
import org.noise_planet.noisemodelling.propagation.ComputeRaysOutAttenuation;
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

public class NoiseModellingProfileReport {
    CRSFactory crsf = new CRSFactory();
    SpatialRefRegistry srr = new SpatialRefRegistry();

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

    public void testDebugNoiseProfile() throws SQLException, IOException, IllegalCoordinateException, CoordinateOperationException, CRSException {

        Logger logger = LoggerFactory.getLogger("debug");
        String workingDir = "/home/nicolas/data/plamade/dep05_1646821826645";
        DataSource ds = NoiseModellingRunner.createDataSource("", "",
                workingDir, "h2gisdb", false);
        try(Connection connection = new ConnectionWrapper(ds.getConnection())) {
            crsf.getRegistryManager().addRegistry(srr);
            srr.setConnection(connection);

            String sourceTable = "LW_ROADS";
            String receiversTable = "RECEIVERS_TEST";
            int configurationId = 4;

            DBTypes dbTypes = DBUtils.getDBType(connection);
            int sridBuildings = GeometryTableUtilities.getSRID(connection, TableLocation.parse("BUILDINGS_SCREENS", dbTypes));


            Coordinate srcPoint = reproject(new Coordinate(5.985848, 44.546319), 4326, sridBuildings);
            Coordinate axeReceiver = reproject(new Coordinate(5.985795, 44.545802), 4326, sridBuildings);
            int distance = 50;
            int step = 5;
            double receiverHeight = 4.0;
            double sourceHeight = 0.5;

            Statement st = connection.createStatement();

            st.execute("DROP TABLE IF EXISTS RECEIVERS_TEST");
            st.execute("CREATE TABLE RECEIVERS_TEST(PK serial primary key, the_geom geometry(POINTZ, " + sridBuildings + "))");

            Vector2D vector2D = new Vector2D(srcPoint, axeReceiver).normalize();

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " +
                    "RECEIVERS_TEST(THE_GEOM) VALUES (?)");
            GeometryFactory factory = new GeometryFactory();
            for(int i = -distance ; i < distance; i += step) {
                if(i != 0) {
                    Coordinate receiverCoordinate = new Coordinate(srcPoint.x +
                            vector2D.multiply(i).getX(), srcPoint.y +
                            vector2D.multiply(i).getY(), receiverHeight);
                    Point geom = factory.createPoint(receiverCoordinate);
                    geom.setSRID(sridBuildings);
                    preparedStatement.setObject(1, geom);
                    preparedStatement.addBatch();
                    preparedStatement.executeBatch();
                }
            }

            PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_SCREENS",
                    sourceTable, receiversTable);

            Sql sql = new Sql(connection);

            GroovyRowResult rs = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", new Object[]{configurationId});
            int reflectionOrder = asInteger(rs.get("confreflorder"));
            int maxSrcDist = asInteger(rs.get("confmaxsrcdist"));
            int maxReflectionDistance = asInteger(rs.get("confmaxrefldist"));
            double wallAlpha = asDouble(rs.get("wall_alpha"));
            // overwrite with the system number of thread - 1
            Runtime runtime = Runtime.getRuntime();
            int nThread = Math.max(1, runtime.availableProcessors() - 1);

            boolean compute_vertical_diffraction = (Boolean)rs.get("confdiffvertical");
            boolean compute_horizontal_diffraction = (Boolean)rs.get("confdiffhorizontal");

            pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction);
            pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction);
            pointNoiseMap.setSoundReflectionOrder(reflectionOrder);


            // Set environmental parameters
            PropagationProcessPathData environmentalData = new PropagationProcessPathData(false);


            GroovyRowResult row_zone = sql.firstRow("SELECT * FROM ZONE");

            double confHumidity = asDouble(row_zone.get("hygro_d"));
            double confTemperature = asDouble(row_zone.get("temp_d"));
            String confFavorableOccurrences = (String)row_zone.get("pfav_06_18");

            environmentalData.setHumidity(confHumidity);
            environmentalData.setTemperature(confTemperature);

            StringTokenizer tk = new StringTokenizer(confFavorableOccurrences, ",");
            double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length];
            for (int i = 0; i < favOccurrences.length; i++) {
                favOccurrences[i] = Math.max(0, Math.min(1, Double.parseDouble(tk.nextToken().trim())));
            }
            environmentalData.setWindRose(favOccurrences);

            pointNoiseMap.setPropagationProcessPathData(environmentalData);

            LDENConfig ldenConfig = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_LW_DEN);

            pointNoiseMap.setPropagationProcessDataFactory(new LDENPointNoiseMapFactory(connection, ldenConfig));

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

            CnossosPropagationData propagationData = pointNoiseMap.prepareCell(connection, 0, 0, new EmptyProgressVisitor(), receivers);

            ComputeCnossosRays computeRays = new ComputeCnossosRays(propagationData);
            computeRays.setThreadCount(1);

            computeRays.makeReceiverRelativeZToAbsolute();

            computeRays.makeSourceRelativeZToAbsolute();

            ComputeRaysOutAttenuation propDataOut = new ComputeRaysOutAttenuation(true, true,
                    pointNoiseMap.getPropagationProcessPathData());

            //Run computation
            computeRays.run(propDataOut);

            Map<Integer, ArrayList<PropagationPath>> propagationPathPerReceiver = new HashMap<>();
            for(PropagationPath propagationPath : propDataOut.propagationPaths) {
                ArrayList<PropagationPath> ar = propagationPathPerReceiver.computeIfAbsent(
                        propagationPath.getIdReceiver(), k1 -> new ArrayList<>());
                ar.add(propagationPath);
                Coordinate receiver = propagationPath.getSRSegment().r;
                Coordinate source = propagationPath.getSRSegment().s;
                logger.info("(Src:"+ propagationPath.getIdSource() + " R:"+propagationPath.getIdReceiver()+") Distance "+   receiver.distance(source) + " m " + Arrays.toString(propagationPath.absorptionData.aGlobal));
            }
        }
    }
}