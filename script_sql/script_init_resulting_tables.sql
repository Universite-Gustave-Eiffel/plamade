--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
-- Initialisation des tables recevant les résultats de noisemodelling_resultats, sur le serveur distant
--
-- Auteur : G. Petit (CNRS - Lab-STICC)
-- Dernière mise à jour : 08/09/21
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/


-- Isophones

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2154;
CREATE TABLE noisemodelling_resultats.cbs_2154 (the_geom geometry (MULTIPOLYGON, 2154), cbstype varchar, typesource varchar,
	indicetype varchar, nutscode varchar, pk varchar, uueid varchar, noiselevel varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2972;
CREATE TABLE noisemodelling_resultats.cbs_2972 (the_geom geometry (MULTIPOLYGON, 2972), cbstype varchar, typesource varchar,
	indicetype varchar, nutscode varchar, pk varchar, uueid varchar, noiselevel varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_2975;
CREATE TABLE noisemodelling_resultats.cbs_2975 (the_geom geometry (MULTIPOLYGON, 2975), cbstype varchar, typesource varchar,
	indicetype varchar, nutscode varchar, pk varchar, uueid varchar, noiselevel varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_4471;
CREATE TABLE noisemodelling_resultats.cbs_4471 (the_geom geometry (MULTIPOLYGON, 4471), cbstype varchar, typesource varchar,
	indicetype varchar, nutscode varchar, pk varchar, uueid varchar, noiselevel varchar);

DROP TABLE IF EXISTS noisemodelling_resultats.cbs_5490;
CREATE TABLE noisemodelling_resultats.cbs_5490 (the_geom geometry (MULTIPOLYGON, 5490), cbstype varchar, typesource varchar,
	indicetype varchar, nutscode varchar, pk varchar, uueid varchar, noiselevel varchar);


-- Exposition des populations

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2154;
CREATE TABLE noisemodelling_resultats.expo_2154 (pk varchar, nutscode varchar, uueid varchar, noiselevel varchar, 
	people integer, area float, dwellings integer, hospitals integer, schools integer, cpi integer, ha integer, 
	hsd integer, begintime date, endtime date);

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2972;
CREATE TABLE noisemodelling_resultats.expo_2972 (pk varchar, nutscode varchar, uueid varchar, noiselevel varchar, 
	people integer, area float, dwellings integer, hospitals integer, schools integer, cpi integer, ha integer, 
	hsd integer, begintime date, endtime date);

DROP TABLE IF EXISTS noisemodelling_resultats.expo_2975;
CREATE TABLE noisemodelling_resultats.expo_2975 (pk varchar, nutscode varchar, uueid varchar, noiselevel varchar, 
	people integer, area float, dwellings integer, hospitals integer, schools integer, cpi integer, ha integer, 
	hsd integer, begintime date, endtime date);

DROP TABLE IF EXISTS noisemodelling_resultats.expo_4471;
CREATE TABLE noisemodelling_resultats.expo_4471 (pk varchar, nutscode varchar, uueid varchar, noiselevel varchar, 
	people integer, area float, dwellings integer, hospitals integer, schools integer, cpi integer, ha integer, 
	hsd integer, begintime date, endtime date);

DROP TABLE IF EXISTS noisemodelling_resultats.expo_5490;
CREATE TABLE noisemodelling_resultats.expo_5490 (pk varchar, nutscode varchar, uueid varchar, noiselevel varchar, 
	people integer, area float, dwellings integer, hospitals integer, schools integer, cpi integer, ha integer, 
	hsd integer, begintime date, endtime date);


-- Métadonnées sur l'upload des données

DROP TABLE IF EXISTS noisemodelling_resultats.metadata;
CREATE TABLE noisemodelling_resultats.metadata (codedept varchar, nutscode varchar, srid integer, insertdate timestamp);