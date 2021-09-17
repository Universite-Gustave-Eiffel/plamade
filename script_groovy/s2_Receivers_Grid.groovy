/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team FROM the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285 
 */

 /* TODO
    - roadWidth depend of the width of each section ? or standard width for railway and road
    - Check spatial index and srids
    - Keep triangles that are over the roads to do not see white areas.
    - Find a good compromise for maxArea parameter
 */

package org.noise_planet.noisemodelling.wps.plamade

import geoserver.GeoServer
import geoserver.catalog.Store

import groovy.sql.Sql
import groovy.time.TimeCategory

import org.geotools.jdbc.JDBCDataStore

import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.h2gis.utilities.JDBCUtilities

import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader

import org.noise_planet.noisemodelling.emission.*
import org.noise_planet.noisemodelling.pathfinder.*
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.jdbc.*

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

title = 'Delaunay and Building Grid'
description = 'Calculates a delaunay grid of receivers based on a single Geometry geom or a table tableName of Geometries with delta as offset in the Cartesian plane in meters and generates receivers placed 2 meters FROM building facades at specified height.'

inputs = [
        confId: [
                name       : 'Global configuration Identifier',
                title      : 'Global configuration Identifier',
                description: 'Id of the global configuration used for this process',
                type: Integer.class
        ]
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]


static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}


def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Delaunay grid and Receivers grid around buildings')
    logger.info("inputs {}", input) // log inputs of the run


    String building_table_name = "BUILDINGS_SCREENS"
    building_table_name = building_table_name.toUpperCase()

    // The receivers height is set to 4.1m
    Double height = 4.1
    // The receivers height is set to 4.1m
    Double h = 4.1

    Sql sql = new Sql(connection)

    // Update metadata table with start time
    sql.execute(String.format("UPDATE metadata SET grid_conf =" + input.confId))
    sql.execute(String.format("UPDATE metadata SET grid_start = NOW();"))
    
    // CREATE TABLE AVEC TOUTES LES SOURCES POUR LA GRILLE DE DELAUNAY
    sql.execute("DROP TABLE IF EXISTS SECTIONS_DELAUNAY;")
    sql.execute("CREATE TABLE SECTIONS_DELAUNAY AS SELECT a.THE_GEOM FROM ROADS a UNION ALL SELECT b.THE_GEOM FROM RAIL_SECTIONS b;")
    sql.execute("ALTER TABLE SECTIONS_DELAUNAY ADD COLUMN PK INT AUTO_INCREMENT PRIMARY KEY;")

    String sources_table_name = "SECTIONS_DELAUNAY"

    def row_conf = sql.firstRow("SELECT * FROM CONF WHERE CONFID = ?", input.containsKey('confId'))
    
    // ----------------------------------------------------------
    // 1- CALCUL DE LA BUILDING_GRID
    // ----------------------------------------------------------

    receivers_table_name = "RECEIVERS_BUILDING"
    receivers_table_name = receivers_table_name.toUpperCase()

    Boolean hasPop = JDBCUtilities.hasField(connection, building_table_name, "POP")
    if (hasPop) {
        logger.info("The building table has a column named POP.")
    }
    if (!hasPop) {
        logger.info("The building table has not a column named POP.")
    }

    if (!JDBCUtilities.hasField(connection, building_table_name, "HEIGHT")) {
        resultString = "Buildings table must have HEIGHT field"
        return resultString
    }

    int delta = row_conf.confDistBuildingsReceivers
    logger.info(String.format("Distance between receivers %d ", delta));

    sql.execute(String.format("DROP TABLE IF EXISTS %s", receivers_table_name))

    // Reproject fence
    int targetSrid = SFSUtilities.getSRID(connection, TableLocation.parse(building_table_name))
    //if (targetSrid == 0 && input['sourcesTableName']) {
    if (targetSrid == 0 && sources_table_name) {
        targetSrid = SFSUtilities.getSRID(connection, TableLocation.parse(sources_table_name))
    }

    Geometry fenceGeom = null

    def buildingPk = JDBCUtilities.getFieldName(connection.getMetaData(), building_table_name, JDBCUtilities.getIntegerPrimaryKey(connection, building_table_name));
    if (buildingPk == "") {
        return "Buildings table must have a primary key"
    }

    sql.execute("DROP TABLE IF EXISTS tmp_receivers_lines")
    def filter_geom_query = ""
    if (fenceGeom != null) {
        filter_geom_query = " WHERE the_geom && ST_GeomFromText('" + fenceGeom + "') AND ST_INTERSECTS(the_geom, ST_GeomFromText('" + fenceGeom + "'))";
    }

    logger.info('Create line of receivers')
    sql.execute("CREATE TABLE tmp_receivers_lines(pk integer not null PRIMARY KEY, the_geom geometry) AS SELECT " + buildingPk + " as pk, st_simplifypreservetopology(ST_ToMultiLine(ST_Buffer(the_geom, 2, 'join=bevel')), 0.05) the_geom FROM " + building_table_name + filter_geom_query)
    sql.execute("CREATE SPATIAL INDEX ON tmp_receivers_lines(the_geom)")

    logger.info('List buildings that will remove receivers (if height is superior than receiver height)')
    sql.execute("DROP TABLE IF EXISTS tmp_relation_screen_building;")
    sql.execute("CREATE TABLE tmp_relation_screen_building AS SELECT b." + buildingPk + " as pk_building, s.pk as pk_screen FROM " + building_table_name + " b, tmp_receivers_lines s WHERE b.the_geom && s.the_geom and s.pk != b.pk and ST_Intersects(b.the_geom, s.the_geom) and b.height > " + h)
    sql.execute("CREATE INDEX ON tmp_relation_screen_building(pk_building);")
    sql.execute("CREATE INDEX ON tmp_relation_screen_building(pk_screen);")
        
    logger.info('Truncate receiver lines')
    sql.execute("DROP TABLE IF EXISTS tmp_screen_truncated;")
    //sql.execute("CREATE TABLE tmp_screen_truncated(pk_screen integer not null PRIMARY KEY, the_geom geometry) AS SELECT r.pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 2)) the_geom FROM tmp_relation_screen_building r, " + building_table_name + " b, tmp_receivers_lines s WHERE pk_building = b." + buildingPk + " AND pk_screen = s.pk  GROUP BY pk_screen, s.the_geom;")

    sql.execute("CREATE TABLE tmp_screen_truncated(pk_screen integer not null, the_geom geometry) AS SELECT r.pk_screen, ST_DIFFERENCE(s.the_geom, ST_BUFFER(ST_ACCUM(b.the_geom), 2)) the_geom FROM tmp_relation_screen_building r, " + building_table_name + " b, tmp_receivers_lines s WHERE pk_building = b." + buildingPk + " AND pk_screen = s.pk  GROUP BY pk_screen, s.the_geom;")

    sql.execute("ALTER TABLE tmp_screen_truncated ADD PRIMARY KEY(pk_screen)")
    
    logger.info('Union of truncated receivers and non tructated')
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS_MERGE;")
    //sql.execute("CREATE TABLE TMP_SCREENS_MERGE (pk integer PRIMARY KEY, the_geom geometry) AS SELECT s.pk, s.the_geom FROM tmp_receivers_lines s WHERE NOT st_isempty(s.the_geom) AND pk NOT IN (SELECT pk_screen FROM tmp_screen_truncated) UNION ALL SELECT pk_screen, the_geom FROM tmp_screen_truncated WHERE NOT st_isempty(the_geom);")

    sql.execute("CREATE TABLE TMP_SCREENS_MERGE (pk integer not null, the_geom geometry) AS SELECT s.pk, s.the_geom the_geom FROM tmp_receivers_lines s WHERE not st_isempty(s.the_geom) and pk not in (select pk_screen FROM tmp_screen_truncated) UNION ALL select pk_screen, the_geom FROM tmp_screen_truncated WHERE not st_isempty(the_geom);")
    //logger.info('Add primary key')
    sql.execute("ALTER TABLE TMP_SCREENS_MERGE ADD PRIMARY KEY(pk)")
    
    logger.info('Collect all lines and convert into points using custom method')
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS;")   
    sql.execute("CREATE TABLE TMP_SCREENS(pk integer, the_geom geometry)")
    def qry = 'INSERT INTO TMP_SCREENS(pk , the_geom) VALUES (?,?);'
    GeometryFactory factory2 = new GeometryFactory(new PrecisionModel(), targetSrid);
    logger.info('Split line to points')
    int nrows2 = sql.firstRow('SELECT COUNT(*) FROM TMP_SCREENS_MERGE')[0] as Integer
    RootProgressVisitor progressLogger2 = new RootProgressVisitor(nrows2, true, 1)
    sql.withBatch(100, qry) { ps ->
        sql.eachRow("SELECT pk, the_geom FROM TMP_SCREENS_MERGE") { row ->
            List<Coordinate> pts = new ArrayList<Coordinate>()
            def geom = row[1] as Geometry
            if (geom instanceof LineString) {
                splitLineStringIntoPoints(geom as LineString, delta, pts)
            } else {
                if (geom instanceof MultiLineString) {
                    for (int idgeom = 0; idgeom < geom.numGeometries; idgeom++) {
                        splitLineStringIntoPoints(geom.getGeometryN(idgeom) as LineString, delta, pts)
                    }
                }
            }
            for (int idp = 0; idp < pts.size(); idp++) {
                Coordinate pt = pts.get(idp);
                if (!Double.isNaN(pt.x) && !Double.isNaN(pt.y)) {
                    // define coordinates of receivers
                    Coordinate newCoord = new Coordinate(pt.x, pt.y, h)
                    ps.addBatch(row[0] as Integer, factory2.createPoint(newCoord))
                }
            }
            progressLogger2.endStep()
        }
    }
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS_MERGE, "+ receivers_table_name)
 

    if (!hasPop) {
        logger.info('Create buildings RECEIVERS table...')

        sql.execute("CREATE TABLE " + receivers_table_name + " (pk integer not null AUTO_INCREMENT, the_geom geometry, build_pk integer)")
        sql.execute("INSERT INTO " + receivers_table_name + " (the_geom, build_pk) select ST_SetSRID(the_geom," + targetSrid.toInteger() + "), pk building_pk FROM TMP_SCREENS;")
        sql.execute("ALTER TABLE " + receivers_table_name + " add primary key(pk)")

        //sql.execute("CREATE TABLE " + receivers_table_name + "(pk integer AUTO_INCREMENT PRIMARY KEY, the_geom geometry, build_pk integer) AS SELECT null, ST_SetSRID(the_geom," + targetSrid.toInteger() + "), pk FROM TMP_SCREENS;")

        if (sources_table_name) {
            sql.execute("CREATE SPATIAL INDEX ON " + sources_table_name + "(the_geom);")
            // Delete receivers near sources
            logger.info('Delete receivers near sources...')
            sql.execute("DELETE FROM " + receivers_table_name + " g WHERE exists (SELECT 1 FROM " + sources_table_name + " r WHERE ST_EXPAND(g.the_geom, 1, 1) && r.the_geom AND ST_DISTANCE(g.the_geom, r.the_geom) < 1 limit 1);")
        }

        if (fenceGeom != null) {
            // Delete receiver not in fence filter
            sql.execute("DELETE FROM " + receivers_table_name + " g WHERE not ST_INTERSECTS(g.the_geom , ST_GeomFromText('" + fenceGeom + "'));")
        }
    } else {
        logger.info('Create RECEIVERS table...')
        // building have population attribute
        // set population attribute divided by number of receiver to each receiver
        sql.execute("DROP TABLE IF EXISTS tmp_receivers;")
        sql.execute("CREATE TABLE tmp_receivers(pk integer not null AUTO_INCREMENT, the_geom geometry, build_pk integer not null);")
        sql.execute("INSERT INTO tmp_receivers(the_geom, build_pk) SELECT ST_SetSRID(the_geom," + targetSrid.toInteger() + "), pk building_pk FROM TMP_SCREENS;")
        sql.execute("ALTER TABLE tmp_receivers ADD PRIMARY KEY(pk)")
        
        //if (input['sourcesTableName']) {
        if (sources_table_name) {
            // Delete receivers near sources
            logger.info('Delete receivers near sources...')
            sql.execute("CREATE SPATIAL INDEX ON " + sources_table_name + "(the_geom);")
            sql.execute("DELETE FROM tmp_receivers g WHERE exists (SELECT 1 FROM " + sources_table_name + " r WHERE ST_EXPAND(g.the_geom, 1) && r.the_geom AND ST_DISTANCE(g.the_geom, r.the_geom) < 1 limit 1);")
        }

        if (fenceGeom != null) {
            // Delete receiver not in fence filter
            sql.execute("DELETE FROM tmp_receivers g WHERE not ST_INTERSECTS(g.the_geom , ST_GeomFromText('" + fenceGeom + "'));")
        }

        sql.execute("CREATE INDEX ON tmp_receivers(build_pk)")
        
        logger.info('Distribute population over receivers')

        // --> Pierre
        sql.execute("CREATE TABLE " + receivers_table_name + "(pk integer not null AUTO_INCREMENT, the_geom geometry, build_pk integer, pop real)")
        sql.execute("INSERT INTO "+receivers_table_name+"(the_geom, build_pk, pop) SELECT a.the_geom, a.build_pk, b.pop/COUNT(DISTINCT aa.pk)::float FROM tmp_receivers a, " + building_table_name + " b,tmp_receivers aa WHERE b." + buildingPk + " = a.build_pk and a.build_pk = aa.build_pk and (b.pop>0 OR b.ERPS_NATUR ='Enseignement' OR b.ERPS_NATUR ='Sante') GROUP BY a.the_geom, a.build_pk, b.pop;")
        sql.execute("ALTER TABLE "+receivers_table_name+" ADD PRIMARY KEY(pk)")

        // --> Gwen
        //sql.execute("CREATE TABLE " + receivers_table_name + "(pk integer IDENTITY, the_geom geometry, build_pk integer, pop float) AS select null, a.the_geom, a.build_pk, b.pop/COUNT(DISTINCT aa.pk)::float FROM tmp_receivers a, " + building_table_name + " b,tmp_receivers aa WHERE b." + buildingPk + " = a.build_pk and a.build_pk = aa.build_pk and (b.pop>0 OR b.ERPS_NATUR ='Enseignement' OR b.ERPS_NATUR ='Sante') GROUP BY a.the_geom, a.build_pk, b.pop;")
        //sql.execute("CREATE TABLE " + receivers_table_name + "(pk integer AUTO_INCREMENT, the_geom geometry, build_pk integer, pop float) AS select null, a.the_geom, a.build_pk, b.pop/COUNT(DISTINCT aa.pk)::float FROM tmp_receivers a, " + building_table_name + " b,tmp_receivers aa WHERE b." + buildingPk + " = a.build_pk and a.build_pk = aa.build_pk and (b.pop>0 OR b.ERPS_NATUR ='Enseignement' OR b.ERPS_NATUR ='Sante') GROUP BY a.the_geom, a.build_pk, b.pop;")
        //sql.execute("ALTER TABLE "+receivers_table_name+" ADD PRIMARY KEY(pk)")


        sql.execute("DROP TABLE IF EXISTS tmp_receivers")
    }

    // Clean non-needed tables
    sql.execute("DROP TABLE TMP_SCREENS, tmp_screen_truncated, tmp_relation_screen_building, tmp_receivers_lines")
    

    // ----------------------------------------------------------
    // 2- CALCUL DE LA DELAUNAY_GRID
    // ----------------------------------------------------------

    logger.info("Creation Delaunay table...")
    connection = new ConnectionWrapper(connection)

    String receivers_table_name = "RECEIVERS_DELAUNAY"
    receivers_table_name = receivers_table_name.toUpperCase()

    Double roadWidth = 2.0

    Double maxArea = 2500

    int srid = SFSUtilities.getSRID(connection, TableLocation.parse(building_table_name))

    Geometry fence = null

    RootProgressVisitor progressLogger = new RootProgressVisitor(1, true, 1)
    
    Double maxPropDist = row_conf.confmaxsrcdist.toDouble()
    logger.info(String.format("PARAM : Maximum source distance equal to %s ", maxPropDist));

    // Delete previous receivers grid
    sql.execute(String.format("DROP TABLE IF EXISTS %s", receivers_table_name))
    sql.execute("DROP TABLE IF EXISTS TRIANGLES_DELAUNAY")

    // Generate receivers grid for noise map rendering
    TriangleNoiseMap noiseMap = new TriangleNoiseMap(building_table_name, sources_table_name)

    // Avoid loading to much geometries when doing Delaunay triangulation
    noiseMap.setMaximumPropagationDistance(maxPropDist)
    // Receiver height relative to the ground
    noiseMap.setReceiverHeight(height)
    // No receivers closer than road width distance
    noiseMap.setRoadWidth(roadWidth)
    // No triangles larger than provided area
    noiseMap.setMaximumArea(maxArea)
    // Densification of receivers near sound sources
    //modifNico noiseMap.setSourceDensification(sourceDensification)

    logger.info("Delaunay initialize")
    noiseMap.initialize(connection, new EmptyProgressVisitor())
    noiseMap.setExceptionDumpFolder("data_dir/")
    AtomicInteger pk = new AtomicInteger(0)
    ProgressVisitor progressVisitorNM = progressLogger.subProcess(noiseMap.getGridDim() * noiseMap.getGridDim())
    noiseMap.setGridDim(25)
    String triangleTable = "TRIANGLES_DELAUNAY"
    for (int i = 0; i < noiseMap.getGridDim(); i++) {
        for (int j = 0; j < noiseMap.getGridDim(); j++) {
            logger.info("Compute cell " + (i * noiseMap.getGridDim() + j + 1) + " of " + noiseMap.getGridDim() * noiseMap.getGridDim())
            noiseMap.generateReceivers(connection, i, j, receivers_table_name, triangleTable, pk)
            progressVisitorNM.endStep()
        }
    }


    //----------------------------
    // 3- MERGE LES DEUX GRILLES
    //------------------------------

    logger.info("Merge Building and Delaunay grids into RECEIVERS table")
    
    sql.execute("DROP TABLE RECEIVERS IF EXISTS;")
    sql.execute("CREATE TABLE RECEIVERS (THE_GEOM geometry, PK integer AUTO_INCREMENT, PK_1 integer, RCV_TYPE integer);")
    sql.execute("INSERT INTO RECEIVERS (THE_GEOM , PK_1 , RCV_TYPE) SELECT THE_GEOM, PK, 2 FROM RECEIVERS_DELAUNAY R WHERE EXISTS (SELECT 1 FROM "+sources_table_name+" S WHERE ST_EXPAND(R.THE_GEOM," + maxPropDist + ", " + maxPropDist + ") && S.THE_GEOM AND ST_DISTANCE(S.THE_GEOM, R.THE_GEOM) < " + maxPropDist + " LIMIT 1 )")
    // as buildings are already filtered with the buffer, we will keep all receivers extracted from buildings
    sql.execute("INSERT INTO RECEIVERS (THE_GEOM , PK_1 , RCV_TYPE) SELECT THE_GEOM, PK, 1 FROM RECEIVERS_BUILDING R")
    sql.execute("ALTER TABLE RECEIVERS ADD PRIMARY KEY(pk)")
    sql.execute("CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM);")
    sql.execute("CREATE INDEX ON RECEIVERS(RCV_TYPE)")

    int nbReceivers = sql.firstRow("SELECT COUNT(*) FROM RECEIVERS")[0] as Integer

    logger.info(String.format(Locale.ROOT, "There is %d receivers", nbReceivers))

    // Remove triangles with missing vertices
    sql.execute("DELETE FROM " + triangleTable + " T WHERE NOT EXISTS (SELECT 1 FROM RECEIVERS WHERE T.PK_1 = R.PK_1 AND RCV_TYPE = 2 LIMIT 1)")
    sql.execute("DELETE FROM " + triangleTable + " T WHERE NOT EXISTS (SELECT 1 FROM RECEIVERS WHERE T.PK_2 = R.PK_1 AND RCV_TYPE = 2 LIMIT 1)")
    sql.execute("DELETE FROM " + triangleTable + " T WHERE NOT EXISTS (SELECT 1 FROM RECEIVERS WHERE T.PK_3 = R.PK_1 AND RCV_TYPE = 2 LIMIT 1)")

    // Ajout des index pour accélerer les jointures à venir, mais pas de gain de temps sur le jeu de test
    // A migrer dans le script 2_Receivers_grid pour ne générer ces index qu'une seule fois par département
    sql.execute("CREATE INDEX ON TRIANGLES_DELAUNAY(PK_1)")
    sql.execute("CREATE INDEX ON TRIANGLES_DELAUNAY(PK_2)")
    sql.execute("CREATE INDEX ON TRIANGLES_DELAUNAY(PK_3)")

    // Process Done
    sql.execute(String.format("UPDATE metadata SET grid_end = NOW();"))
    resultString = "Process done. Receivers tables created."

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Building and Delaunay grid')

    // print to WPS Builder
    return resultString
}


/**
 *
 * @param geom Geometry
 * @param segmentSizeConstraint Maximal distance between points
 * @param [out]pts computed points
 * @return Fixed distance between points
 */
double splitLineStringIntoPoints(LineString geom, double segmentSizeConstraint,
                                 List<Coordinate> pts) {
    // If the linear sound source length is inferior than half the distance between the nearest point of the sound
    // source and the receiver then it can be modelled as a single point source
    double geomLength = geom.getLength();
    if (geomLength < segmentSizeConstraint) {
        // Return mid point
        Coordinate[] points = geom.getCoordinates();
        double segmentLength = 0;
        final double targetSegmentSize = geomLength / 2.0;
        for (int i = 0; i < points.length - 1; i++) {
            Coordinate a = points[i];
            final Coordinate b = points[i + 1];
            double length = a.distance3D(b);
            if (length + segmentLength > targetSegmentSize) {
                double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                Coordinate midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                        a.y + segmentLengthFraction * (b.y - a.y),
                        a.z + segmentLengthFraction * (b.z - a.z));
                pts.add(midPoint);
                break;
            }
            segmentLength += length;
        }
        return geom.getLength();
    } else {
        double targetSegmentSize = geomLength / Math.ceil(geomLength / segmentSizeConstraint);
        Coordinate[] points = geom.getCoordinates();
        double segmentLength = 0.0;

        // Mid point of segmented line source
        def midPoint = null;
        for (int i = 0; i < points.length - 1; i++) {
            Coordinate a = points[i];
            final Coordinate b = points[i + 1];
            double length = a.distance3D(b);
            if (Double.isNaN(length)) {
                length = a.distance(b);
            }
            while (length + segmentLength > targetSegmentSize) {
                //LineSegment segment = new LineSegment(a, b);
                double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                Coordinate splitPoint = new Coordinate();
                splitPoint.x = a.x + segmentLengthFraction * (b.x - a.x);
                splitPoint.y = a.y + segmentLengthFraction * (b.y - a.y);
                splitPoint.z = a.z + segmentLengthFraction * (b.z - a.z);
                if (midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                    segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                    midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                            a.y + segmentLengthFraction * (b.y - a.y),
                            a.z + segmentLengthFraction * (b.z - a.z));
                }
                pts.add(midPoint);
                a = splitPoint;
                length = a.distance3D(b);
                if (Double.isNaN(length)) {
                    length = a.distance(b);
                }
                segmentLength = 0;
                midPoint = null;
            }
            if (midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                double segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                        a.y + segmentLengthFraction * (b.y - a.y),
                        a.z + segmentLengthFraction * (b.z - a.z));
            }
            segmentLength += length;
        }
        if (midPoint != null) {
            pts.add(midPoint);
        }
        return targetSegmentSize;
    }
}