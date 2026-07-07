package org.noise_planet.covadis.scripts.CBS

import groovy.sql.Sql
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.noise_planet.covadis.webserver.database.PostGISUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.sql.Connection

title = 'Full NoiseModelling computation that output results for each UUEID separately'
description = 'Full NoiseModelling computation that output results for each UUEID separately'

inputs = [
        projectionName: [
                name: "Projection name",
                title: "Projection name",
                description: "Projection name",
                allowedValues: ["hexa", "guad", "guya", "mart", "reun"],
                type: String.class
        ] ,
        uueid_pattern: [
                title: "UUEID pattern",
                name: "UUEID pattern",
                description: "UUEID pattern on roads to extract. <p>A percent sign % - represents zero, one, or multiple characters</p>" +
                        "<p>A underscore sign _ - represents a single character</p>",
                type: String.class
        ],
        conf: [
                title: "Configuration identifier",
                name: "Configuration identifier",
                description: "Configuration identifier defined in cbs_uge_input.nm_conf ",
                type: Integer.class
        ]
]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'Result table name. Can be used as input for another WPS process', type: String.class]]

def exec(Connection connection, Map input, ProgressVisitor progress) {
    Logger logger = LoggerFactory.getLogger(this.class)

    // Fetch PostGIS connection settings from the configuration table
    Sql h2sql = new Sql(connection)
    if(!JDBCUtilities.tableExists(connection, "POSTGIS_CONFIGURATION")) {
        throw new RuntimeException("The table POSTGIS_CONFIGURATION does not exist. Please run the Write_PostGIS_Settings process first to create and fill this table with the connection settings to the PostGIS database.")
    }

    def postgisConfig = h2sql.firstRow("SELECT * FROM POSTGIS_CONFIGURATION")

    def mainConfiguration = [:]
    Geometry extractionEnvelopeGeometry = null
    try (DataSource dataSource = PostGISUtilities.createPostgisDataSource(
            postgisConfig['user_name'] as String,
            postgisConfig['password'] as String,
            postgisConfig['port'].toString(), postgisConfig['database_name'] as String, postgisConfig['host'] as String);
         Connection pgConnection = dataSource.getConnection()) {
        logger.info("Connected to PostgreSQL database")
        pgConnection.setAutoCommit(true)
        Sql sql = new Sql(pgConnection)

        // Fetch configuration
        // CREATE TABLE cbs_uge_input.nm_conf (
        //	confid int4 NOT NULL,
        //	confreflorder int4 NULL,
        //	confmaxsrcdist int4 NULL,
        //	confmaxrefldist int4 NULL,
        //	confdistbuildingsreceivers int4 NULL,
        //	confthreadnumber int4 NULL,
        //	confdiffvertical bool NULL,
        //	confdiffhorizontal bool NULL,
        //	confskiplday bool NULL,
        //	confskiplevening bool NULL,
        //	confskiplnight bool NULL,
        //	confskiplden bool NULL,
        //	confexportsourceid bool NULL,
        //	wall_alpha float4 NULL,
        //	CONSTRAINT nm_conf_pkey PRIMARY KEY (confid)
        //);
        mainConfiguration = sql.firstRow("SELECT * FROM cbs_uge_input.nm_conf WHERE confid = ${input.conf}")

        List<String> uueids = new ArrayList<>()
        sql.rows("SELECT DISTINCT uueid from cbs_uge_output.routier_emission_${input.projectionName}" as String).each {
            row ->
                uueids.add(row.uueid as String)
        }
        ProgressVisitor stepsProgress = progress.subProcess(uueids.size()) // long running sub tasks

        if(uueids.isEmpty()) {
            throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}")
        }

        uueids.each {
            computeForUUEID(it, connection, pgConnection, stepsProgress, input, mainConfiguration)
        }

        // Return results
        return [result : "OK"]
    }


}

def computeForUUEID(String uueid, Connection h2Connection, Connection pgConnection, ProgressVisitor progress, Map input, Map mainConfiguration) {
    ProgressVisitor stepsProgress = progress.subProcess(3) // long running sub tasks
    def pgSql = new Sql(pgConnection)
    def res = pgSql.firstRow("""SELECT 
             st_simplify(st_buffer(st_convexhull(st_collect(the_geom)), ${mainConfiguration.confmaxsrcdist * 1.2 + mainConfiguration.confmaxrefldist}), 25) geomenv
             FROM cbs_uge_output.routier_emission_${input.projectionName} AS reg WHERE uueid LIKE '$uueid';""" as String)
    if(res == null) {
        throw new IllegalArgumentException("No match for the provided uueid '${input.uueid_pattern}'")
    }
    def extractionEnvelopeGeometry = res.geomenv as Geometry

    def tableQuery = """SELECT b.geom3d as the_geom, b.bat_haut as height, b.idbat, b.pop_bat as pop
             FROM cbs_uge_input.c_batiment_s_${input.projectionName} b 
                INNER JOIN cbs_uge_input.c_population_${input.projectionName} p ON b.idbat = c.idbat  
             WHERE ST_Intersect(geom3d, '$extractionEnvelopeGeometry'::geometry)"""

    new Copy_PostGIS_To_H2GIS().exec(h2Connection, [tableToExport: "($tableQuery)" as String, tableName: "BUILDINGS_GEOM"], stepsProgress)

}
