------------------------------------------------------------------------------------
-- Script d'alimentation de NoiseModelling à partir de la base de données Plamade --
-- Auteur : Gwendall Petit (Lab-STICC - CNRS UMR 6285)                            --
-- Dernière mise à jour : 02/2021                                                 --
------------------------------------------------------------------------------------


--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
--
-- Select needed data, for a specific department
--
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/

---------------------------------------------------------------------------------
-- 1- Create a dedicated working schema
---------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS noisemodelling_work;


---------------------------------------------------------------------------------
-- 2- Select the studied departement and generate a 1km buffer around
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS noisemodelling_work.dept;
CREATE TABLE noisemodelling_work.dept AS SELECT ST_BUFFER(the_geom, 1000) as the_geom, id, nom_dep_m, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.ign_admin_express_dept_l93 
	WHERE insee_dep='38';

CREATE INDEX dept_geom_idx ON noisemodelling_work.dept USING gist (the_geom);


---------------------------------------------------------------------------------
-- 3- Select the closest station from the (centroid of the) studied department
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS noisemodelling_work.dept_pfav;
CREATE TABLE noisemodelling_work.dept_pfav AS SELECT b.insee_dep, a.*   
	FROM noisemodelling.station_pfav a, noisemodelling_work.dept b 
	ORDER BY st_distance(a.the_geom, st_centroid(b.the_geom)) LIMIT 1;

DROP TABLE IF EXISTS noisemodelling_work.dept_meteo;
CREATE TABLE noisemodelling_work.dept_meteo AS SELECT codedept, temp_d, temp_e, temp_n, hygro_d, hygro_e, hygro_n, wall_alpha, ts_stud, pm_stud 
	FROM echeance4."C_METEO_S_FRANCE" 
	WHERE codedept='038';

DROP TABLE IF EXISTS noisemodelling_work.zone;
CREATE TABLE noisemodelling_work.zone AS SELECT 
	a.pfav_06_18, a.pfav_18_22, a.pfav_22_06, a.pfav_06_22, 
	b.temp_d, b.temp_e, b.temp_n, b.hygro_d, b.hygro_e, b.hygro_n, b.wall_alpha, b.ts_stud, b.pm_stud 
	FROM noisemodelling_work.dept_pfav a, noisemodelling_work.dept_meteo b;


DROP TABLE IF EXISTS noisemodelling_work.dept_pfav, noisemodelling_work.dept_meteo;
---------------------------------------------------------------------------------
-- 4- Select and format the roads
---------------------------------------------------------------------------------

-- A z-value is added and set at 5 centimetres above the ground.
DROP TABLE IF EXISTS noisemodelling_work.roads;
CREATE TABLE noisemodelling_work.roads 
AS SELECT 
	st_translate(st_force3dz(a.the_geom), 0, 0, 0.05) as the_geom,
	a."IDTRONCON" as id_troncon, 
	a."IDROUTE" as id_route, 
	f."NOMROUTE" as nom_route,
	b."TMHVLD" as lv_d, 
	b."TMHVLS" as lv_e, 
	b."TMHVLN" as lv_n,
	b."TMHPLD" * b."PCENTMPL" as mv_d,
	b."TMHPLS" * b."PCENTMPL" as mv_e,
	b."TMHPLN" * b."PCENTMPL" as mv_n,
	b."TMHPLD" * b."PCENTHPL" as hgv_d,
	b."TMHPLS" * b."PCENTHPL" as hgv_e,
	b."TMHPLN" * b."PCENTHPL" as hgv_n,
	b."TMH2RD" * b."PCENT2R4A" as wav_d,
	b."TMH2RS" * b."PCENT2R4A" as wav_e,
	b."TMH2RN" * b."PCENT2R4A" as wav_n,
	b."TMH2RD" * b."PCENT2R4B" as wbv_d,
	b."TMH2RS" * b."PCENT2R4B" as wbv_e,
	b."TMH2RN" * b."PCENT2R4B" as wbv_n,
	c."VITESSEVL" as lv_spd_d,
	c."VITESSEVL" as lv_spd_e,
	c."VITESSEVL" as lv_spd_n,
	c."VITESSEPL" as mv_spd_d,
	c."VITESSEPL" as mv_spd_e,
	c."VITESSEPL" as mv_spd_n,
	c."VITESSEPL" as hgv_spd_d,
	c."VITESSEPL" as hgv_spd_e,
	c."VITESSEPL" as hgv_spd_n,
	c."VITESSEVL" as wav_spd_d,
	c."VITESSEVL" as wav_spd_e,
	c."VITESSEVL" as wav_spd_n,
	c."VITESSEVL" as wbv_spd_d,
	c."VITESSEVL" as wbv_spd_e,
	c."VITESSEVL" as wbv_spd_n,
	d."REVETEMENT" as revetement,
	d."GRANULO" as granulo,
	d."CLASSACOU" as classacou,
	ROUND((a."ZFIN"-a."ZDEB")/ ST_LENGTH(a.the_geom)*100) as slope, 
	a."ZDEB" as z_start, 
	a."ZFIN" as z_end,
	a."SENS" as sens,
	(CASE 	WHEN a."SENS" = '01' THEN '01' 
			WHEN a."SENS" = '02' THEN '02' 
			ELSE '03'
	 END) as way
FROM 
	noisemodelling."N_ROUTIER_TRONCON_L_l93" a,
	echeance4."N_ROUTIER_TRAFIC" b,
	echeance4."N_ROUTIER_VITESSE" c,
	echeance4."N_ROUTIER_REVETEMENT" d, 
	noisemodelling_work.dept e,
	echeance4."N_ROUTIER_ROUTE" f  
WHERE 
	a."CBS_GITT"='O' and
	a."IDTRONCON"=b."IDTRONCON" and
	a."IDTRONCON"=c."IDTRONCON" and
	a."IDTRONCON"=d."IDTRONCON" and 
	a."IDROUTE"=f."IDROUTE" and
	a.the_geom && e.the_geom and 
	ST_INTERSECTS(a.the_geom, e.the_geom);


ALTER TABLE noisemodelling_work.roads ADD COLUMN pvmt varchar(4);
UPDATE noisemodelling_work.roads b SET pvmt = a.pvmt FROM noisemodelling.pvmt a WHERE a.revetement=b.revetement AND a.granulo=b.granulo AND a.classacou=b.classacou;
-- Add a primary
ALTER TABLE noisemodelling_work.roads ADD COLUMN pk serial PRIMARY KEY;
-- Create a spatial index
CREATE INDEX roads_geom_idx ON noisemodelling_work.roads USING gist (the_geom);


---------------------------------------------------------------------------------
-- 5- Select and format the buildings
---------------------------------------------------------------------------------

-- Generate a unique 1km buffer area around the roads
DROP TABLE IF EXISTS noisemodelling_work.roads_buff;
CREATE TABLE noisemodelling_work.roads_buff AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, 1000))) as the_geom FROM noisemodelling_work.roads;
CREATE INDEX roads_buff_geom_idx ON noisemodelling_work.roads_buff USING gist (the_geom);

-- Select the buildings that intersects the buffer area
DROP TABLE IF EXISTS noisemodelling_work.buildings;
CREATE TABLE noisemodelling_work.buildings AS SELECT 
	a.the_geom, 
	a."IDBAT" as id_bat, 
	a."BAT_HAUT" as height,
	b."POP_BAT" as pop
FROM 
	noisemodelling."C_BATIMENT_S_l93" a,
	echeance4."C_POPULATION" b,
	noisemodelling_work.roads_buff c 
where
	a.the_geom && c.the_geom and 
	ST_INTERSECTS(a.the_geom, c.the_geom) and 
	a."IDBAT"=b."IDBAT";

-- Add a primary key
ALTER TABLE noisemodelling_work.buildings ADD COLUMN pk serial PRIMARY KEY;


---------------------------------------------------------------------------------
-- 6- Remove unnecessary tables
---------------------------------------------------------------------------------

DROP TABLE noisemodelling_work.dept, noisemodelling_work.roads_buff;
