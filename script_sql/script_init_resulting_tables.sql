--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
-- Initialisation des tables recevant les résultats de noisemodelling_resultats, sur le serveur distant
--
-- Auteur : G. Petit (Cerema - UMRAE)
-- Dernière mise à jour : 13/04/22
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/


-- Isophones

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2154;
CREATE TABLE noisemodelling_resultats.cbs_2154 (the_geom geometry (MULTIPOLYGON, 2154), cbstype varchar, typesource varchar,
	indicetype varchar, codedept varchar, pk varchar, uueid varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2972;
CREATE TABLE noisemodelling_resultats.cbs_2972 (the_geom geometry (MULTIPOLYGON, 2972), cbstype varchar, typesource varchar,
	indicetype varchar, codedept varchar, pk varchar, uueid varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2975;
CREATE TABLE noisemodelling_resultats.cbs_2975 (the_geom geometry (MULTIPOLYGON, 2975), cbstype varchar, typesource varchar,
	indicetype varchar, codedept varchar, pk varchar, uueid varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_4471;
CREATE TABLE noisemodelling_resultats.cbs_4471 (the_geom geometry (MULTIPOLYGON, 4471), cbstype varchar, typesource varchar,
	indicetype varchar, codedept varchar, pk varchar, uueid varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_5490;
CREATE TABLE noisemodelling_resultats.cbs_5490 (the_geom geometry (MULTIPOLYGON, 5490), cbstype varchar, typesource varchar,
	indicetype varchar, codedept varchar, pk varchar, uueid varchar, category varchar, source varchar);


-- Isophones agrégées

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_agr_2154;
CREATE TABLE noisemodelling_resultats.cbs_agr_2154 (the_geom geometry (MULTIPOLYGON, 2154), pk varchar, cbstype varchar, 
typesource varchar, indicetype varchar, codedept varchar, legende varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_agr_2972;
CREATE TABLE noisemodelling_resultats.cbs_agr_2972 (the_geom geometry (MULTIPOLYGON, 2972), pk varchar, cbstype varchar, 
typesource varchar, indicetype varchar, codedept varchar, legende varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_agr_2975;
CREATE TABLE noisemodelling_resultats.cbs_agr_2975 (the_geom geometry (MULTIPOLYGON, 2975), pk varchar, cbstype varchar, 
typesource varchar, indicetype varchar, codedept varchar, legende varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_agr_4471;
CREATE TABLE noisemodelling_resultats.cbs_agr_4471 (the_geom geometry (MULTIPOLYGON, 4471), pk varchar, cbstype varchar, 
typesource varchar, indicetype varchar, codedept varchar, legende varchar, category varchar, source varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_agr_5490;
CREATE TABLE noisemodelling_resultats.cbs_agr_5490 (the_geom geometry (MULTIPOLYGON, 5490), pk varchar, cbstype varchar, 
typesource varchar, indicetype varchar, codedept varchar, legende varchar, category varchar, source varchar);


-- Exposition des populations

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2154;
CREATE TABLE noisemodelling_resultats.expo_2154(pk varchar, estatunitcode varchar, uueid varchar, exposuretype varchar,
 category varchar, source varchar, exposedpeople int DEFAULT(0), exposedarea real DEFAULT(0), exposeddwellings int DEFAULT(0),
 exposedhospitals int DEFAULT(0), exposedschools int DEFAULT(0), cpi int DEFAULT(0), ha int DEFAULT(0), hsd int DEFAULT(0));

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2972;
CREATE TABLE noisemodelling_resultats.expo_2972(pk varchar, estatunitcode varchar, uueid varchar, exposuretype varchar,
 category varchar, source varchar, exposedpeople int DEFAULT(0), exposedarea real DEFAULT(0), exposeddwellings int DEFAULT(0),
 exposedhospitals int DEFAULT(0), exposedschools int DEFAULT(0), cpi int DEFAULT(0), ha int DEFAULT(0), hsd int DEFAULT(0));

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2975;
CREATE TABLE noisemodelling_resultats.expo_2975(pk varchar, estatunitcode varchar, uueid varchar, exposuretype varchar,
 category varchar, source varchar, exposedpeople int DEFAULT(0), exposedarea real DEFAULT(0), exposeddwellings int DEFAULT(0),
 exposedhospitals int DEFAULT(0), exposedschools int DEFAULT(0), cpi int DEFAULT(0), ha int DEFAULT(0), hsd int DEFAULT(0));

DROP TABLE IF EXISTS noisemodelling_resultats.expo_4471;
CREATE TABLE noisemodelling_resultats.expo_4471(pk varchar, estatunitcode varchar, uueid varchar, exposuretype varchar,
 category varchar, source varchar, exposedpeople int DEFAULT(0), exposedarea real DEFAULT(0), exposeddwellings int DEFAULT(0),
 exposedhospitals int DEFAULT(0), exposedschools int DEFAULT(0), cpi int DEFAULT(0), ha int DEFAULT(0), hsd int DEFAULT(0));

DROP TABLE IF EXISTS noisemodelling_resultats.expo_5490;
CREATE TABLE noisemodelling_resultats.expo_5490(pk varchar, estatunitcode varchar, uueid varchar, exposuretype varchar,
 category varchar, source varchar, exposedpeople int DEFAULT(0), exposedarea real DEFAULT(0), exposeddwellings int DEFAULT(0),
 exposedhospitals int DEFAULT(0), exposedschools int DEFAULT(0), cpi int DEFAULT(0), ha int DEFAULT(0), hsd int DEFAULT(0));


-- Métadonnées sur l'upload des données

DROP TABLE IF EXISTS noisemodelling_resultats.metadata;
CREATE TABLE noisemodelling_resultats.metadata (codedept varchar, nutscode varchar, srid integer, insertdate timestamp);


