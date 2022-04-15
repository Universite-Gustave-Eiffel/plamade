/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/**
 * @Author Gwendall Petit, Lab-STICC CNRS UMR 6285
 * @Author Sylvain Palominos, Cerema 
 */


package org.noise_planet.noisemodelling.wps.plamade

import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.GroovyResultSet
import groovy.sql.GroovyResultSetProxy
import groovy.text.SimpleTemplateEngine
import groovy.transform.CompileStatic
import org.locationtech.jts.geom.Point
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.Geometry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.sql.Sql
import java.sql.Connection
import java.sql.Statement
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation

import org.locationtech.jts.io.WKTWriter



title = 'Export results'
description = 'Export NoiseModelling results into the Cerema remote db'

inputs = [
        databaseUser : [
                name       : 'PostGIS user',
                title      : 'PostGIS username',
                description: 'PostGIS username for authentication',
                type       : String.class
        ],
        databasePassword : [
                name       : 'PostGIS password',
                title      : 'PostGIS password',
                description: 'PostGIS password for authentication',
                type       : String.class
        ],
        batchSize : [
                name       : 'Batch size',
                title      : 'Batch size',
                description: 'Batch size. Default 1000',
                type       : Integer.class
        ],
        inputServer : [
                name       : 'DB Server used',
                title      : 'DB server used',
                description: 'Choose between cerema or cloud',
                type       : String.class
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

def getPostgreConnection(user, password, url) {
    def driver = 'org.h2gis.postgis_jts.Driver'
    def sql = Sql.newInstance(url, user, password, driver)
    return sql
}

@CompileStatic
def doExport(Sql sqlH2gis, Sql sqlPostgre, String table_cbs, String codeDep,int srid, int batchSize, String codeNuts) {
    def writer = new WKTWriter(3)

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    Map<String, String> noiseLevelToLegende = [
            'Lden5559' : '55',
            'Lden6064' : '60',
            'Lden6569' : '65',
            'Lden7074' : '70',
            'LdenGreaterThan75' : '75',
            'LdenGreaterThan68' : '68',
            'LdenGreaterThan73' : '73',
            'Lnight5054' : '50',
            'Lnight5559' : '55',
            'Lnight6064' : '60',
            'Lnight6569' : '65',
            'LnightGreaterThan70' : '70',
            'LnightGreaterThan62' : '62',
            'LnightGreaterThan65' : '65'
    ]
    int autoIncrement = 0;
    for(cbstype in ['A', 'C']) {
        for(typesource in ['R', 'F']) {
            String sourcepg = typesource == 'R' ? 'majorRoadsIncludingAgglomeration' : 'majorRailwaysIncludingAgglomeration'
            for (indicetype in ['LD', 'LN']) {
                // on génère le nom de la table à partir des éléments ci-dessus
                def inputTableCBS = "CBS_" + cbstype + "_" + typesource + "_" + indicetype + "_" + codeDep

                // on teste si la table existe dans NM
                if ((sqlH2gis.firstRow("SELECT count(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE table_name ='" + inputTableCBS + "';")[0] as Integer) > 0) {
                    logger.info("La table $inputTableCBS va être exportée dans la table $table_cbs")
                    sqlPostgre.withBatch(batchSize, 'INSERT INTO noisemodelling_resultats.' + table_cbs + ' (the_geom, cbstype, typesource, indicetype, codedept, pk, uueid, category, source) VALUES (ST_SetSRID(ST_GeomFromText(?), ?), ?, ?, ?, ?, ?, ?, ?, ?)'.toString()) { BatchingPreparedStatementWrapper ps ->
                        sqlH2gis.eachRow("SELECT ST_Polygonize(the_geom) as the_geom, pk, uueid, noiselevel FROM " + inputTableCBS + ";") {
                            GroovyResultSet row -> ps.addBatch(writer.write(row[0] as Geometry), srid, cbstype, typesource, indicetype, codeDep, row[1], row[2], row[3], sourcepg)
                        }
                    }
                    logger.info("La carte du bruit $inputTableCBS a été exportée sur le serveur")
                } else {
                    logger.info("La table $inputTableCBS n'existe pas")
                }

                // Send merged table (no UUEID column)
                def inputMergedTableCBS = "CBS_" + cbstype + "_" + typesource + "_" + indicetype + "_" + codeDep + "_MERGED"

                // on teste si la table existe dans NM
                if (JDBCUtilities.tableExists(sqlH2gis.getConnection(), inputMergedTableCBS)) {
                    logger.info("La table $inputMergedTableCBS va être exportée dans la table cbs_agr_" + srid)
                    sqlPostgre.withBatch(batchSize, 'INSERT INTO noisemodelling_resultats.cbs_agr_' + srid + ' (the_geom, cbstype, typesource, indicetype, codedept, pk, category, source, legende) VALUES (ST_SetSRID(ST_GeomFromText(?), ?), ?, ?, ?, ?, ?, ?, ?, ?)'.toString()) { BatchingPreparedStatementWrapper ps ->
                        sqlH2gis.eachRow("SELECT the_geom, noiselevel FROM " + inputMergedTableCBS + ";") {
                            GroovyResultSet row -> ps.addBatch(writer.write(row[0] as Geometry), srid, cbstype, typesource, indicetype, codeDep, codeDep+"_"+(autoIncrement++), row[1] as String, sourcepg, noiseLevelToLegende[row[1] as String])
                        }
                    }
                    logger.info("La carte du bruit $inputMergedTableCBS a été exportée sur le serveur")
                } else {
                    logger.info("La table $inputMergedTableCBS n'existe pas")
                }
            }
        }
    }

    sqlPostgre.execute("DELETE FROM noisemodelling_resultats.expo_"+srid+" WHERE ESTATUnitCode='" + codeNuts + "'")

    sqlPostgre.withBatch(batchSize, 'INSERT INTO noisemodelling_resultats.expo_'+srid+' (PK,ESTATUnitCode,UUEID, EXPOSURETYPE, NOISELEVEL,EXPOSEDPEOPLE, EXPOSEDAREA, EXPOSEDDWELLINGS, EXPOSEDHOSPITALS , EXPOSEDSCHOOLS , CPI , HA, HSD) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'.toString()) { BatchingPreparedStatementWrapper ps ->
        sqlH2gis.eachRow("SELECT * FROM POPULATION_EXPOSURE;"){ GroovyResultSet row ->
         ps.addBatch(row[0],row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
        }
    }

}

def exec(Connection connection, input) {

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")


    // Get provided parameters
    def databaseUrl
    if(input["inputServer"].equals('cerema')) {
        databaseUrl="jdbc:postgresql_h2://161.48.203.166:5432/plamade?ssl=true&sslmode=prefer"
    } else if(input["inputServer"].equals('cloud')) {
        databaseUrl = "jdbc:postgresql_h2://57.100.98.126:5432/plamade?ssl=true&sslmode=prefer"
    } else{
        return "Vous n'avez pas spécifié le bon nom de serveur"
    }

    def user = input["databaseUser"] as String
    def pwd = input["databasePassword"] as String


    // On déclare les deux connections aux bdd
    def sqlPostgre = getPostgreConnection(user, pwd, databaseUrl)
    def sqlH2gis = new Sql(connection)

    def writer = new WKTWriter()


    // On stocke dans les variables codeDep et codeNuts les informations relatives au département qui a été traité (présent dans la table metadata)
    def codeDep = sqlH2gis.firstRow("SELECT code_dept FROM metadata;").code_dept
    String codeNuts = sqlH2gis.firstRow("SELECT nuts FROM metadata;").nuts as String

    // Declare table variables depending on the department and the projection system
    def srid = 2154
    def table_cbs = "cbs_2154"
    def table_expo = "expo_2154"

    if(codeDep=='971' || codeDep=='972') {
        srid=5490
        table_cbs = "cbs_5490"
        table_expo = "expo_5490"
    }
    else if(codeDep=='973') {
        srid=2972
        table_cbs = "cbs_2972"
        table_expo = "expo_2972"
    }
    else if(codeDep=='974') {
            srid=2975
            table_cbs = "cbs_2975"
            table_expo = "expo_2975"
        }
        else if(codeDep=='976') {
                srid=4471
                table_cbs = "cbs_4471"
                table_expo = "expo_4471"
            }



    // print to command window
    logger.info("Début de l'export vers la base PostGIS sur le serveur")


    if(sqlPostgre.firstRow("SELECT count(*) as count FROM noisemodelling_resultats.metadata WHERE codedept='$codeDep';").count>0) {
        // On supprime les données déjà existantes
        logger.info("Le département $codeDep existe déjà dans la base. Suppression des données existantes")
        sqlPostgre.execute """
        DELETE FROM noisemodelling_resultats."$table_cbs" WHERE codedept = '$codeDep';
        DELETE FROM noisemodelling_resultats."$table_expo" WHERE estatunitcode = '$codeNuts';
        DELETE FROM noisemodelling_resultats.metadata WHERE nutscode = '$codeNuts';
        """
        logger.info("Les données relatives au département $codeDep ont été supprimées de la base")


    } //end if

    logger.info("Début de l'export du département $codeDep sur le serveur")

    // On insère les nouvelles données dans la table des metadata
    logger.info("Mise à jour de la table des métadonnées")
    sqlPostgre.withBatch(input["batchSize"] as int, 'INSERT INTO noisemodelling_resultats.metadata (codedept, nutscode, srid, insertdate) VALUES (?, ?, ?, ?)') { ps ->
        sqlH2gis.eachRow("SELECT code_dept, nuts, srid, NOW() FROM metadata;"){row ->
            ps.addBatch(row[0], row[1], row[2], row[3])
        }
    }

    // On insère les nouvelles données dans la table des CBS
    logger.info("Export des CBS")

    doExport(sqlH2gis, sqlPostgre, table_cbs as String, codeDep as String, srid as Integer, input["batchSize"] as Integer, codeNuts)

    logger.info("Les cartes du bruit du département $codeDep ont été exporté sur le serveur")

    return "Les cartes du bruit du département $codeDep ont été exporté sur le serveur"

}

