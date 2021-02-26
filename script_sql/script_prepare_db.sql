------------------------------------------------------------------------------------
-- Script d'alimentation de NoiseModelling à partir de la base de données Plamade --
-- Auteur : Gwendall Petit (Lab-STICC - CNRS UMR 6285)                            --
-- Dernière mise à jour : 02/2021                                                 --
------------------------------------------------------------------------------------


--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
--
-- DB initialization (to be executed only once)
--
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/

---------------------------------------------------------------------------------
-- 1- Create the noisemodelling schema in which all working tables will be stored
---------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS noisemodelling;


---------------------------------------------------------------------------------
-- 2- Reproject Cerema tables into Lambert 93
---------------------------------------------------------------------------------

-- For roads
DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_l93";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_l93" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT"
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX n_routier_troncon_l_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_l93" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_l93" USING gist (the_geom);

-- For buildings
DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_l93";
CREATE TABLE noisemodelling."C_BATIMENT_S_l93" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX c_batiment_s_idbat_idx ON noisemodelling."C_BATIMENT_S_l93" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_geom_idx ON noisemodelling."C_BATIMENT_S_l93" USING gist (the_geom);



---------------------------------------------------------------------------------
-- 3- Generate configuration and parameters tables
---------------------------------------------------------------------------------

----------------------------------
-- CONF table
DROP TABLE IF EXISTS noisemodelling.conf;
CREATE TABLE noisemodelling.conf (confId integer Primary Key, confReflOrder integer, confMaxSrcDist integer, confMaxReflDist integer, 
	confDistBuildingsReceivers integer, confThreadNumber integer, confDiffVertical boolean, confDiffHorizontal boolean, 
	confSkipLday boolean, confSkipLevening boolean, confSkipLnight boolean, confSkipLden boolean, confExportSourceId boolean);
COMMENT ON COLUMN noisemodelling.conf.confId IS 'Configuration identifier';
COMMENT ON COLUMN noisemodelling.conf.confReflOrder IS 'Order of reflexion';
COMMENT ON COLUMN noisemodelling.conf.confMaxSrcDist IS 'Maximum distance between source and receiver (in meters)';
COMMENT ON COLUMN noisemodelling.conf.confMaxReflDist IS 'Maximum reflection distance from the source (in meters)';
COMMENT ON COLUMN noisemodelling.conf.confDistBuildingsReceivers IS 'Distance between receivers in the Cartesian plane (in meters)';
COMMENT ON COLUMN noisemodelling.conf.confThreadNumber IS 'Number of thread to use on the computer';
COMMENT ON COLUMN noisemodelling.conf.confDiffVertical IS 'Compute or not the diffraction on vertical edges';
COMMENT ON COLUMN noisemodelling.conf.confDiffHorizontal IS 'Compute or not the diffraction on horizontal edges';
COMMENT ON COLUMN noisemodelling.conf.confSkipLday IS 'Skip the creation of Lday table';
COMMENT ON COLUMN noisemodelling.conf.confSkipLevening IS 'Skip the creation of Levening table';
COMMENT ON COLUMN noisemodelling.conf.confSkipLnight IS 'Skip the creation of the Lnight table';
COMMENT ON COLUMN noisemodelling.conf.confSkipLden IS 'Skip the creation of the Lden table';
COMMENT ON COLUMN noisemodelling.conf.confExportSourceId IS 'Keep source identifier in output in order to get noise contribution of each noise source';

-- Insert values
INSERT INTO noisemodelling.conf VALUES(1, 0, 250, 50, 5, 1, false, false, true, true, true, false, true);
INSERT INTO noisemodelling.conf VALUES(2, 0, 250, 50, 5, 1, false, false, true, true, true, false, false);

----------------------------------
-- CONF_ROAD table
DROP TABLE IF EXISTS noisemodelling.conf_road;
CREATE TABLE noisemodelling.conf_road (confId integer Primary Key, junc_dist float8, junc_type integer);
COMMENT ON COLUMN noisemodelling.conf_road.confId IS 'Configuration identifier';
COMMENT ON COLUMN noisemodelling.conf_road.junc_dist IS 'Distance to junction in meters';
COMMENT ON COLUMN noisemodelling.conf_road.junc_type IS 'Type of junction (k=0 none, k = 1 for a crossing with traffic lights, k = 2 for a roundabout)';

-- Insert values
INSERT INTO noisemodelling.conf_road VALUES(1, 200, 0);

----------------------------------
-- CONF_RAIL table
DROP TABLE IF EXISTS noisemodelling.conf_rail;
CREATE TABLE noisemodelling.conf_rail (confId integer Primary Key, id_plateform varchar, entre_axe integer);
COMMENT ON COLUMN noisemodelling.conf_rail.confId IS 'Configuration identifier';
COMMENT ON COLUMN noisemodelling.conf_rail.id_plateform IS 'Foreign key to the plateform table';
COMMENT ON COLUMN noisemodelling.conf_rail.entre_axe IS 'Default width of the centre-to-centre distance (in metres)';

----------------------------------
-- Road's pavement
DROP TABLE IF EXISTS noisemodelling.pvmt;
CREATE TABLE noisemodelling.pvmt AS SELECT DISTINCT "REVETEMENT" as revetement, "GRANULO" as granulo, "CLASSACOU" as classacou, count(*) as total 
FROM echeance4."N_ROUTIER_REVETEMENT" GROUP BY ("REVETEMENT", "GRANULO", "CLASSACOU"); 

ALTER TABLE noisemodelling.pvmt ADD COLUMN pvmt varchar(4) DEFAULT 'FR2N';
UPDATE noisemodelling.pvmt SET pvmt='FR1N' WHERE classacou='R1';
UPDATE noisemodelling.pvmt SET pvmt='FR2D' WHERE revetement='BBDr' or revetement='BBDR';
UPDATE noisemodelling.pvmt SET pvmt='FR3N' WHERE classacou='R3';




----------------------------------
-- Stations and PFVA

-- Select the cities that are listed in the "Road noise prediction" document made by Sétra
SELECT st_centroid(the_geom) as the_geom, nom, id, code_insee FROM ign_bdtopo_2017.commune WHERE 
   nom like 'Abbeville'
or nom like 'Aix-en-Provence'
or nom like 'Avord'
or nom like 'Bordeaux'
or nom like 'Brest'
or nom like 'Caen'
or nom like 'Carcassonne'
or nom like 'Carpentras'
or nom like 'Dijon'
or nom like 'Dinard'
or nom like 'Dunkerque'
or nom like 'Évreux'
or nom like 'Fréjus'
or nom like 'Gourdon' and code_insee ='46127' 
or nom like 'La Rochelle' and code_insee ='17300'
or nom like 'Laval' and code_insee ='53130' 
or nom like 'Lille'
or nom like 'Limoges'
or nom like 'Lorient'
or nom like 'Luxeuil-les-Bains'
or nom like 'Lyon'
or nom like 'Mâcon'
or nom like 'Mont-de-Marsan'
or nom like 'Montélimar'
or nom like 'Montpellier'
or nom like 'Nancy'
or nom like 'Nantes'
or nom like 'Nice'
or nom like 'Nîmes'
or nom like 'Orléans'
or nom like 'Pau'
or nom like 'Perpignan'
or nom like 'Poitiers' 
or nom like 'Reims'
or nom like 'Rennes'
or nom like 'Saint-Dizier'
or nom like 'Saint-Quentin'
or nom like 'Strasbourg'
or nom like 'Toulouse'
or nom like 'Tours'
or nom like 'Valognes'
order by nom
;


-- Create the station table, with their POINT geometry
DROP TABLE IF EXISTS noisemodelling.station;
CREATE TABLE noisemodelling.station (the_geom geometry (POINT, 2154), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(616373.3907217126 7001744.583853569)', 2154),'Abbeville','SURFCOMM0000000088879389','80001');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(893919.0293355002 6273725.142621572)', 2154),'Aix-en-Provence','SURFCOMM0000000041520664','13001');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(674607.5701839515 6659131.366549444)', 2154),'Avord','SURFCOMM0000000028066429','18018');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(417799.9976869353 6423990.918437644)', 2154),'Bordeaux','SURFCOMM0000000052189828','33063');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(145530.00273641292 6837477.3869183585)', 2154),'Brest','SURFCOMM0000000030001423','29019');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(454371.5118771473 6903601.663448182)', 2154),'Caen','SURFCOMM0000000028305288','14118');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(646608.1611191704 6234689.5157902)', 2154),'Carcassonne','SURFCOMM0000000082026644','11069');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(865149.1206673698 6331088.0969790565)', 2154),'Carpentras','SURFCOMM0000000039657837','84031');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(853877.7920067526 6693376.86887768)', 2154),'Dijon','SURFCOMM0000000053362260','21231');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(327178.87603466475 6848017.307727337)', 2154),'Dinard','SURFCOMM0000000049126777','35093');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(653363.0240544211 7104019.048392724)', 2154),'Dunkerque','SURFCOMM0000000256883843','59183');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(564075.9883230523 6881586.733291183)', 2154),'Évreux','SURFCOMM0000000082908132','27229');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(1004552.3764158675 6270914.394926301)', 2154),'Fréjus','SURFCOMM0000000075370462','83061');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(571502.1816703166 6405288.202799344)', 2154),'Gourdon','SURFCOMM0000000034543660','46127');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(378105.88037093764 6571019.596784672)', 2154),'La Rochelle','SURFCOMM0000000035782630','17300');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(419583.9515651179 6780101.639063162)', 2154),'Laval','SURFCOMM0000000029679566','53130');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(703330.7171756831 7059432.721058839)', 2154),'Lille','SURFCOMM0000000057564909','59350');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(564123.4842605114 6529811.830314347)', 2154),'Limoges','SURFCOMM0000000034695959','87085');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(222544.97974805092 6758220.078977364)', 2154),'Lorient','SURFCOMM0000000087808502','56121');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(951634.272585947 6752395.312970484)', 2154),'Luxeuil-les-Bains','SURFCOMM0000000048265697','70311');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(842740.5769202277 6518916.568179048)', 2154),'Lyon','SURFCOMM0000000025564961','69123');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(839914.1670596567 6581750.749848031)', 2154),'Mâcon','SURFCOMM0000000043956231','71270');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(419640.23345633235 6317366.443854319)', 2154),'Mont-de-Marsan','SURFCOMM0000000030165836','40192');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(838860.8363883786 6385394.293790727)', 2154),'Montélimar','SURFCOMM0000000026655997','26198');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(770192.1200213472 6279753.20414169)', 2154),'Montpellier','SURFCOMM0000000046595170','34172');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(933662.8902057507 6848022.89884269)', 2154),'Nancy','SURFCOMM0000000054047095','54395');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(356071.7575110152 6691211.887896891)', 2154),'Nantes','SURFCOMM0000000029999037','44109');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(1041459.8849840319 6299535.359835112)', 2154),'Nice','SURFCOMM0000000075358432','06088');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(808376.2272716055 6305993.907192477)', 2154),'Nîmes','SURFCOMM0000000036253934','30189');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(619024.1861362043 6754096.477293332)', 2154),'Orléans','SURFCOMM0000000028065065','45234');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(428173.3901694165 6252531.915747433)', 2154),'Pau','SURFCOMM0000000028107759','64445');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(691711.445832622 6177430.403179263)', 2154),'Perpignan','SURFCOMM0000000039658911','66136');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(497854.1963258736 6612669.423452804)', 2154),'Poitiers','SURFCOMM0000002001872004','86194');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(775739.6248968422 6906283.006674417)', 2154),'Reims','SURFCOMM0000000066200172','51454');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(351768.41152412945 6789360.740158863)', 2154),'Rennes','SURFCOMM0000000049126597','35238');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(843642.9525351524 6838224.707560948)', 2154),'Saint-Dizier','SURFCOMM0000000066995929','52448');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(719990.6623170109 6972100.243269524)', 2154),'Saint-Quentin','SURFCOMM0000000064722261','02691');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(1051530.0080132915 6840730.093252666)', 2154),'Strasbourg','SURFCOMM0000000051649648','67482');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(573360.7726835464 6278698.498061831)', 2154),'Toulouse','SURFCOMM0000000073650926','31555');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(526250.5969930588 6702318.560193463)', 2154),'Tours','SURFCOMM0000000035784550','37261');
INSERT INTO noisemodelling.station VALUES(ST_GeomFromText('POINT(376070.4581367768 6944038.247843531)', 2154),'Valognes','SURFCOMM0000000029538809','50615');

-- Create and feed the PFAV table
DROP TABLE IF EXISTS noisemodelling.pfav;
CREATE TABLE  noisemodelling.pfav(
   station        VARCHAR(17) NOT NULL,
   insee_station  VARCHAR(5) NOT NULL PRIMARY KEY,
   pfav_06_18_020 INTEGER  NOT NULL, pfav_06_18_040 INTEGER  NOT NULL, pfav_06_18_060 INTEGER  NOT NULL, pfav_06_18_080 INTEGER  NOT NULL, pfav_06_18_100 INTEGER  NOT NULL, 
   pfav_06_18_120 INTEGER  NOT NULL, pfav_06_18_140 INTEGER  NOT NULL, pfav_06_18_160 INTEGER  NOT NULL, pfav_06_18_180 INTEGER  NOT NULL, pfav_06_18_200 INTEGER  NOT NULL, 
   pfav_06_18_220 INTEGER  NOT NULL, pfav_06_18_240 INTEGER  NOT NULL, pfav_06_18_260 INTEGER  NOT NULL, pfav_06_18_280 INTEGER  NOT NULL, pfav_06_18_300 INTEGER  NOT NULL, 
   pfav_06_18_320 INTEGER  NOT NULL, pfav_06_18_340 INTEGER  NOT NULL, pfav_06_18_360 INTEGER  NOT NULL,
   pfav_18_22_020 INTEGER  NOT NULL, pfav_18_22_040 INTEGER  NOT NULL, pfav_18_22_060 INTEGER  NOT NULL, pfav_18_22_080 INTEGER  NOT NULL, pfav_18_22_100 INTEGER  NOT NULL, 
   pfav_18_22_120 INTEGER  NOT NULL, pfav_18_22_140 INTEGER  NOT NULL, pfav_18_22_160 INTEGER  NOT NULL, pfav_18_22_180 INTEGER  NOT NULL, pfav_18_22_200 INTEGER  NOT NULL, 
   pfav_18_22_220 INTEGER  NOT NULL, pfav_18_22_240 INTEGER  NOT NULL, pfav_18_22_260 INTEGER  NOT NULL, pfav_18_22_280 INTEGER  NOT NULL, pfav_18_22_300 INTEGER  NOT NULL, 
   pfav_18_22_320 INTEGER  NOT NULL, pfav_18_22_340 INTEGER  NOT NULL, pfav_18_22_360 INTEGER  NOT NULL, 
   pfav_22_06_020 INTEGER  NOT NULL, pfav_22_06_040 INTEGER  NOT NULL, pfav_22_06_060 INTEGER  NOT NULL, pfav_22_06_080 INTEGER  NOT NULL, pfav_22_06_100 INTEGER  NOT NULL, 
   pfav_22_06_120 INTEGER  NOT NULL, pfav_22_06_140 INTEGER  NOT NULL, pfav_22_06_160 INTEGER  NOT NULL, pfav_22_06_180 INTEGER  NOT NULL, pfav_22_06_200 INTEGER  NOT NULL, 
   pfav_22_06_220 INTEGER  NOT NULL, pfav_22_06_240 INTEGER  NOT NULL, pfav_22_06_260 INTEGER  NOT NULL, pfav_22_06_280 INTEGER  NOT NULL, pfav_22_06_300 INTEGER  NOT NULL, 
   pfav_22_06_320 INTEGER  NOT NULL, pfav_22_06_340 INTEGER  NOT NULL, pfav_22_06_360 INTEGER  NOT NULL, 
   pfav_06_22_020 INTEGER  NOT NULL, pfav_06_22_040 INTEGER  NOT NULL, pfav_06_22_060 INTEGER  NOT NULL, pfav_06_22_080 INTEGER  NOT NULL, pfav_06_22_100 INTEGER  NOT NULL, 
   pfav_06_22_120 INTEGER  NOT NULL, pfav_06_22_140 INTEGER  NOT NULL, pfav_06_22_160 INTEGER  NOT NULL, pfav_06_22_180 INTEGER  NOT NULL, pfav_06_22_200 INTEGER  NOT NULL, 
   pfav_06_22_220 INTEGER  NOT NULL, pfav_06_22_240 INTEGER  NOT NULL, pfav_06_22_260 INTEGER  NOT NULL, pfav_06_22_280 INTEGER  NOT NULL, pfav_06_22_300 INTEGER  NOT NULL, 
   pfav_06_22_320 INTEGER  NOT NULL, pfav_06_22_340 INTEGER  NOT NULL, pfav_06_22_360 INTEGER  NOT NULL
);
INSERT INTO noisemodelling.pfav(station,insee_station,
	pfav_06_18_020,pfav_06_18_040,pfav_06_18_060,pfav_06_18_080,pfav_06_18_100,pfav_06_18_120,pfav_06_18_140,pfav_06_18_160,pfav_06_18_180,
	pfav_06_18_200,pfav_06_18_220,pfav_06_18_240,pfav_06_18_260,pfav_06_18_280,pfav_06_18_300,pfav_06_18_320,pfav_06_18_340,pfav_06_18_360,
	pfav_18_22_020,pfav_18_22_040,pfav_18_22_060,pfav_18_22_080,pfav_18_22_100,pfav_18_22_120,pfav_18_22_140,pfav_18_22_160,pfav_18_22_180,
	pfav_18_22_200,pfav_18_22_220,pfav_18_22_240,pfav_18_22_260,pfav_18_22_280,pfav_18_22_300,pfav_18_22_320,pfav_18_22_340,pfav_18_22_360,
	pfav_22_06_020,pfav_22_06_040,pfav_22_06_060,pfav_22_06_080,pfav_22_06_100,pfav_22_06_120,pfav_22_06_140,pfav_22_06_160,pfav_22_06_180,
	pfav_22_06_200,pfav_22_06_220,pfav_22_06_240,pfav_22_06_260,pfav_22_06_280,pfav_22_06_300,pfav_22_06_320,pfav_22_06_340,pfav_22_06_360,
	pfav_06_22_020,pfav_06_22_040,pfav_06_22_060,pfav_06_22_080,pfav_06_22_100,pfav_06_22_120,pfav_06_22_140,pfav_06_22_160,pfav_06_22_180,
	pfav_06_22_200,pfav_06_22_220,pfav_06_22_240,pfav_06_22_260,pfav_06_22_280,pfav_06_22_300,pfav_06_22_320,pfav_06_22_340,pfav_06_22_360) VALUES
 ('Abbeville','80001',34,30,28,30,33,36,39,43,45,48,52,55,54,53,51,47,42,39,60,51,46,44,43,43,44,43,44,49,58,64,65,64,63,64,64,64,43,40,40,42,44,48,52,56,58,61,64,64,61,59,55,52,48,46,41,35,33,33,35,38,40,43,45,48,54,57,57,56,54,51,48,45), 
 ('Aix-en-Provence','13001',32,33,31,28,26,26,26,26,26,25,22,22,26,31,33,34,33,32,64,63,57,46,37,33,34,38,45,53,57,59,68,72,72,70,68,65,64,72,69,62,53,47,46,44,43,42,36,33,38,42,45,48,51,56,39,39,37,32,28,27,28,28,30,31,30,30,35,40,42,42,41,39), 
 ('Avord','18018',25,25,25,26,26,26,27,28,29,30,31,32,32,32,31,30,28,27,63,58,54,51,49,47,46,46,47,49,52,55,57,60,63,66,68,67,48,46,46,46,46,46,47,49,51,52,52,51,50,50,51,51,51,50,34,32,31,31,31,31,31,32,33,34,36,37,37,38,38,38,37,36), 
 ('Bordeaux','33063',33,31,32,35,35,35,34,35,39,43,45,44,43,42,42,42,40,37,65,60,54,49,46,43,39,37,40,46,53,58,60,62,66,71,72,70,49,45,44,44,43,42,42,43,48,55,59,61,60,60,62,63,61,55,41,38,38,38,38,37,36,36,39,43,47,48,47,47,48,49,48,45), 
 ('Brest','29019',39,36,33,31,31,32,35,38,42,46,49,52,53,52,49,46,42,41,57,52,47,44,42,41,42,43,45,49,53,57,61,64,66,65,65,63,49,45,43,42,42,43,46,49,51,54,56,59,60,60,59,57,55,52,43,40,36,34,34,34,36,40,43,47,50,53,55,55,53,50,48,46), 
 ('Caen','14118',33,29,28,29,29,31,34,39,45,50,55,56,55,54,52,49,44,39,58,51,50,49,47,44,42,41,44,50,56,57,58,62,64,68,68,64,39,33,33,33,34,37,42,49,55,63,69,71,70,68,66,60,54,47,39,34,34,34,34,34,36,40,44,50,55,56,56,56,55,54,50,45), 
 ('Carcassonne','11069',47,34,30,30,31,31,31,30,27,21,36,51,57,58,58,57,55,52,82,62,44,38,37,37,38,40,43,45,56,63,65,65,65,65,66,71,67,44,34,34,36,38,39,41,47,58,69,69,68,68,66,64,63,64,55,41,33,32,32,33,33,33,31,27,41,54,59,60,60,59,58,56), 
 ('Carpentras','84031',36,34,32,26,22,22,22,23,23,23,23,22,26,34,38,39,38,37,67,67,64,54,36,33,36,40,44,47,50,53,66,77,77,73,70,68,54,65,72,68,60,58,58,57,54,49,41,34,36,39,39,39,40,45,43,41,39,32,25,24,25,27,28,29,29,29,35,44,47,46,45,44), 
 ('Dijon','21231',38,37,36,33,29,26,28,32,35,37,39,40,41,42,41,37,37,38,57,55,53,52,51,53,53,51,50,50,52,55,58,62,66,65,62,59,60,58,56,52,44,36,32,35,37,39,41,44,50,60,70,70,65,62,43,42,40,38,35,33,34,37,39,41,42,43,45,47,47,44,43,43), 
 ('Dinard','35093',37,36,34,31,30,32,35,41,46,49,49,51,53,53,51,47,42,38,61,60,58,52,46,44,44,45,46,45,46,48,53,59,62,63,64,62,42,42,41,39,38,41,48,55,60,61,60,61,63,64,61,54,48,43,43,42,40,36,34,35,38,42,46,48,48,50,53,55,53,51,48,44), 
 ('Dunkerque','59183',34,32,30,30,32,35,39,44,47,50,53,54,55,52,48,43,39,37,51,48,46,45,46,48,48,52,54,55,57,59,59,59,59,59,57,54,39,39,39,40,44,49,55,60,63,63,63,63,62,59,53,48,42,40,38,36,34,34,35,38,41,46,49,51,54,56,56,54,50,47,44,41), 
 ('Evreux','27229',34,31,29,28,29,31,35,39,42,45,49,52,53,52,50,44,40,37,57,53,48,46,44,45,48,49,49,51,55,59,63,65,65,62,60,59,49,45,42,40,40,41,46,49,51,54,58,62,64,65,63,59,55,53,40,36,34,32,33,34,38,42,44,47,50,54,56,55,53,49,45,43), 
 ('Fréjus','83061',47,41,34,28,25,25,27,27,25,21,23,28,33,39,43,45,46,46,58,57,53,55,58,60,62,62,61,60,65,66,62,57,52,50,50,54,94,89,74,56,36,22,17,15,14,15,30,52,72,84,87,88,90,92,49,44,38,34,32,33,34,35,33,30,32,36,39,43,45,46,47,48), 
 ('Gourdon','46127',23,20,20,23,26,29,32,35,38,41,42,42,39,37,35,32,30,26,61,57,55,53,49,45,45,46,49,56,63,69,71,70,70,68,66,64,36,38,46,53,56,58,61,64,67,72,75,72,60,52,47,43,40,38,32,29,28,30,31,33,35,37,40,44,47,48,46,45,43,41,38,35), 
 ('La Rochelle','17300',40,39,37,36,37,37,35,33,31,33,36,39,41,41,40,40,41,41,69,62,53,43,39,38,37,35,34,37,43,51,63,68,69,71,73,74,71,68,63,59,57,54,48,43,39,37,40,44,48,51,54,61,67,71,47,45,41,38,37,38,36,33,32,34,38,42,46,48,47,48,49,49), 
 ('Laval','53130',33,33,33,32,32,31,32,36,40,43,44,45,46,46,46,44,40,36,54,53,50,47,44,44,44,48,52,53,54,56,60,63,66,66,63,57,53,52,51,48,45,39,38,41,46,49,51,52,55,61,67,67,63,58,38,38,37,36,35,34,35,39,43,45,47,48,49,50,51,49,46,41), 
 ('Lille','59350',32,29,27,27,29,33,38,41,45,51,55,58,58,56,51,44,40,36,52,47,45,45,44,45,48,49,51,58,61,63,64,66,66,63,63,60,45,41,39,40,42,45,49,51,55,59,63,65,64,63,60,58,56,51,37,33,31,31,32,36,41,43,47,53,57,59,59,58,55,49,46,42), 
 ('Limoges','87085',34,34,35,36,37,39,40,40,40,40,42,42,41,39,36,34,33,34,61,55,52,51,50,50,48,46,46,49,54,57,60,63,65,67,68,66,64,60,58,58,58,57,55,53,48,45,47,48,48,48,50,54,60,64,40,39,39,39,40,42,42,42,41,42,45,46,46,45,43,42,41,41), 
 ('Lorient','56121',41,39,38,37,36,35,34,34,34,38,44,47,48,49,49,47,45,43,53,45,41,39,37,36,36,38,41,53,62,65,67,67,68,69,68,64,64,57,51,48,46,43,41,38,36,38,44,49,52,55,58,61,64,67,44,41,39,38,37,36,35,35,36,42,48,51,52,53,53,53,51,48), 
 ('Luxeuil-les-Bains','70311',23,22,24,26,28,29,33,39,43,44,44,42,41,40,39,36,30,25,54,54,56,56,54,54,56,60,63,66,68,67,64,63,64,63,59,55,51,56,59,61,61,61,63,66,65,58,50,44,40,39,40,39,40,45,29,29,31,33,34,35,38,43,47,49,49,48,46,45,45,42,36,31), 
 ('Lyon','69123',41,41,40,36,28,29,32,34,34,34,33,32,31,34,35,36,38,40,65,66,68,69,64,58,51,46,44,43,43,44,47,60,66,64,63,64,53,55,57,57,49,49,50,50,51,51,51,52,57,70,68,59,55,53,47,47,47,44,37,36,37,37,37,36,35,35,35,40,43,43,45,46), 
 ('Mâcon','71270',39,36,30,24,22,26,30,31,32,32,33,35,39,39,37,38,40,40,68,68,67,62,57,49,43,40,39,40,45,55,65,72,73,70,69,69,69,67,60,42,32,31,31,31,32,35,42,59,78,83,79,73,71,70,45,43,38,32,30,31,32,33,33,34,36,39,45,46,45,45,46,46), 
 ('Mont-de-Marsan','40192',27,28,29,30,31,31,31,33,35,36,36,37,38,39,39,37,33,29,64,59,51,46,43,43,43,46,50,55,59,60,61,62,64,67,68,67,48,46,44,42,41,41,42,46,50,52,50,51,52,53,54,53,51,49,36,35,34,34,34,33,34,36,38,40,41,42,43,44,45,44,41,38), 
 ('Montélimar','26198',64,65,64,63,60,48,25,17,17,18,19,18,17,16,18,34,54,62,74,73,73,74,78,76,51,35,32,31,31,32,34,40,57,77,79,75,79,79,79,80,83,79,54,31,24,22,22,22,23,27,44,71,80,80,66,67,67,66,64,55,31,21,21,21,22,21,21,22,27,44,60,65), 
 ('Montpellier','34172',52,47,43,40,37,32,26,24,24,25,27,30,31,33,37,45,50,53,56,52,48,46,46,47,48,51,53,55,60,65,65,65,66,66,62,59,82,77,69,61,54,47,35,26,23,27,36,45,52,59,69,79,84,84,53,49,44,41,39,36,32,30,31,32,35,38,39,41,44,50,53,54), 
 ('Nancy','54395',31,30,29,29,30,35,39,40,41,44,46,48,48,46,41,35,33,32,59,56,53,51,52,56,59,59,56,54,57,60,62,63,62,60,60,61,44,44,44,46,50,55,60,58,56,55,56,56,54,51,46,42,44,45,38,36,34,34,36,40,44,44,45,46,48,51,51,50,46,41,39,39), 
 ('Nantes','44109',35,34,33,33,35,37,37,37,37,40,44,46,46,44,42,40,37,37,56,47,43,42,41,42,43,45,46,52,58,62,64,65,66,67,66,64,55,51,48,47,47,49,49,49,47,49,52,55,56,56,56,59,60,59,40,37,35,36,37,38,39,39,39,43,48,50,50,49,48,46,44,43), 
 ('Nice','06088',48,50,50,40,25,24,25,26,25,22,18,17,26,42,45,46,46,46,56,61,62,62,52,49,52,55,56,51,44,45,54,56,55,53,50,50,92,92,90,79,31,18,15,13,13,11,12,18,54,82,86,88,89,90,50,53,53,46,32,30,32,33,33,28,24,24,33,45,48,48,47,47), 
 ('Nîmes','30189',54,55,53,49,42,32,26,22,20,19,18,17,19,23,31,40,47,52,64,63,63,63,62,58,52,48,47,47,48,51,54,59,66,70,69,66,87,88,87,84,73,52,35,27,23,20,21,25,38,56,73,80,83,86,57,57,56,52,46,38,32,28,26,25,25,25,27,32,39,47,52,55), 
 ('Orléans','45234',35,34,34,34,34,34,36,38,41,45,48,49,49,48,46,41,37,36,55,51,49,48,48,49,52,53,52,53,56,57,59,61,62,61,60,59,49,47,46,47,47,47,50,51,50,52,54,55,54,54,55,54,52,51,40,38,38,38,37,38,40,42,44,47,50,51,52,51,50,46,43,42), 
 ('Pau','64445',28,26,27,28,29,29,28,29,31,35,38,38,38,39,39,39,38,34,71,62,54,48,45,43,43,45,48,54,60,64,65,66,68,71,74,74,38,40,45,48,49,50,53,57,65,72,74,69,60,54,51,49,46,41,38,34,33,33,32,32,31,32,35,39,43,44,44,45,46,47,46,43), 
 ('Perpignan','66136',54,51,39,26,24,23,22,20,20,19,20,28,43,51,55,56,56,55,76,77,70,51,43,40,39,37,35,33,33,46,60,65,67,69,71,74,77,69,53,30,21,18,18,20,26,33,41,63,79,84,86,87,87,83,59,58,46,32,28,27,26,24,23,23,23,32,47,55,58,59,60,60), 
 ('Poitiers','86194',34,33,31,29,28,30,33,35,39,43,46,47,49,49,46,41,38,35,54,50,47,46,45,45,45,45,49,52,54,57,59,61,62,64,64,61,47,45,42,38,37,37,40,42,46,50,53,55,59,61,61,59,56,52,39,37,35,33,32,33,36,38,41,45,48,50,51,52,50,46,44,41), 
 ('Reims','51454',29,28,27,25,25,28,30,36,43,48,50,52,53,52,50,48,41,33,51,49,47,46,46,44,46,50,56,59,60,61,64,66,66,66,61,55,42,40,38,35,35,38,42,50,58,61,62,64,67,69,66,62,54,46,35,33,32,30,30,32,34,40,47,51,52,54,56,55,54,52,46,38), 
 ('Rennes','35238',32,31,31,30,30,31,35,39,43,45,47,48,48,48,46,42,38,34,53,53,52,51,48,45,45,49,52,53,53,53,55,59,64,66,60,55,48,45,43,42,40,40,43,47,51,54,56,57,59,61,62,60,55,51,37,36,36,35,35,35,37,42,45,47,48,49,50,50,50,48,43,39), 
 ('Saint-Dizier','52448',26,29,30,31,32,35,39,44,49,49,48,48,48,47,44,38,31,26,55,53,53,52,51,52,56,59,62,61,61,62,64,65,66,64,59,56,42,49,51,51,53,58,64,70,73,70,61,55,53,51,48,43,37,36,33,35,36,36,37,39,43,48,52,52,51,51,52,51,49,44,38,33), 
 ('Saint-Quentin','02691',32,31,31,32,33,35,36,39,44,48,50,50,50,49,47,44,41,36,54,51,49,48,47,48,48,48,49,53,57,60,61,61,61,62,61,58,49,48,48,48,48,48,47,48,51,53,55,55,54,54,53,55,55,52,38,36,35,36,37,38,39,41,45,49,52,53,53,52,51,48,46,42), 
 ('Strasbourg','67482',31,29,26,23,22,25,30,34,39,42,45,46,47,45,40,36,35,34,58,54,51,49,50,53,52,48,49,52,56,60,65,70,71,70,67,63,47,43,39,36,35,40,45,49,53,57,61,66,73,77,73,64,58,51,38,35,32,29,28,32,35,38,41,45,47,49,51,51,47,44,43,41), 
 ('Toulouse','31555',34,22,17,22,26,28,31,32,33,38,46,49,47,48,47,46,45,42,63,52,48,49,43,39,38,38,40,49,61,69,68,67,67,68,67,67,44,30,27,32,34,39,43,46,49,60,75,82,77,72,67,61,57,53,41,29,25,28,30,31,32,33,35,41,50,54,52,52,52,51,50,48), 
 ('Tours','37261',34,34,34,34,35,37,39,39,40,43,45,46,46,46,43,39,35,34,56,52,50,48,46,47,49,50,51,53,55,58,60,61,62,62,62,59,52,50,50,49,47,47,49,50,49,50,51,52,53,55,55,54,54,54,39,38,38,38,38,39,41,42,43,45,47,49,49,49,48,44,42,40), 
 ('Valognes','50615',37,35,33,32,30,30,31,33,35,38,41,45,48,49,49,46,42,39,61,54,49,45,44,43,44,45,46,48,53,58,61,64,65,65,65,64,58,52,47,43,40,38,39,42,44,47,51,55,59,63,66,65,63,61,43,40,37,35,34,33,34,36,38,40,44,48,51,52,53,51,48,45);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_06_18 varchar;
UPDATE noisemodelling.pfav SET pfav_06_18 = ROUND((pfav_06_18_020::numeric/100),2) || ', '|| ROUND((pfav_06_18_040::numeric/100),2) || ', '|| ROUND((pfav_06_18_060::numeric/100),2) || ', '|| ROUND((pfav_06_18_080::numeric/100),2) || ', '|| ROUND((pfav_06_18_100::numeric/100),2) || ', '|| ROUND((pfav_06_18_120::numeric/100),2) || ', '|| ROUND((pfav_06_18_140::numeric/100),2) || ', '|| ROUND((pfav_06_18_160::numeric/100),2) || ', '|| ROUND((pfav_06_18_180::numeric/100),2) || ', '|| ROUND((pfav_06_18_200::numeric/100),2) || ', '|| ROUND((pfav_06_18_220::numeric/100),2) || ', '|| ROUND((pfav_06_18_240::numeric/100),2) || ', '|| ROUND((pfav_06_18_260::numeric/100),2) || ', '|| ROUND((pfav_06_18_280::numeric/100),2) || ', '|| ROUND((pfav_06_18_300::numeric/100),2) || ', '|| ROUND((pfav_06_18_320::numeric/100),2) || ', '|| ROUND((pfav_06_18_340::numeric/100),2) || ', '|| ROUND((pfav_06_18_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_18_22 varchar;
UPDATE noisemodelling.pfav SET pfav_18_22 = ROUND((pfav_18_22_020::numeric/100),2) || ', '|| ROUND((pfav_18_22_040::numeric/100),2) || ', '|| ROUND((pfav_18_22_060::numeric/100),2) || ', '|| ROUND((pfav_18_22_080::numeric/100),2) || ', '|| ROUND((pfav_18_22_100::numeric/100),2) || ', '|| ROUND((pfav_18_22_120::numeric/100),2) || ', '|| ROUND((pfav_18_22_140::numeric/100),2) || ', '|| ROUND((pfav_18_22_160::numeric/100),2) || ', '|| ROUND((pfav_18_22_180::numeric/100),2) || ', '|| ROUND((pfav_18_22_200::numeric/100),2) || ', '|| ROUND((pfav_18_22_220::numeric/100),2) || ', '|| ROUND((pfav_18_22_240::numeric/100),2) || ', '|| ROUND((pfav_18_22_260::numeric/100),2) || ', '|| ROUND((pfav_18_22_280::numeric/100),2) || ', '|| ROUND((pfav_18_22_300::numeric/100),2) || ', '|| ROUND((pfav_18_22_320::numeric/100),2) || ', '|| ROUND((pfav_18_22_340::numeric/100),2) || ', '|| ROUND((pfav_18_22_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_22_06 varchar;
UPDATE noisemodelling.pfav SET pfav_22_06 = ROUND((pfav_22_06_020::numeric/100),2) || ', '|| ROUND((pfav_22_06_040::numeric/100),2) || ', '|| ROUND((pfav_22_06_060::numeric/100),2) || ', '|| ROUND((pfav_22_06_080::numeric/100),2) || ', '|| ROUND((pfav_22_06_100::numeric/100),2) || ', '|| ROUND((pfav_22_06_120::numeric/100),2) || ', '|| ROUND((pfav_22_06_140::numeric/100),2) || ', '|| ROUND((pfav_22_06_160::numeric/100),2) || ', '|| ROUND((pfav_22_06_180::numeric/100),2) || ', '|| ROUND((pfav_22_06_200::numeric/100),2) || ', '|| ROUND((pfav_22_06_220::numeric/100),2) || ', '|| ROUND((pfav_22_06_240::numeric/100),2) || ', '|| ROUND((pfav_22_06_260::numeric/100),2) || ', '|| ROUND((pfav_22_06_280::numeric/100),2) || ', '|| ROUND((pfav_22_06_300::numeric/100),2) || ', '|| ROUND((pfav_22_06_320::numeric/100),2) || ', '|| ROUND((pfav_22_06_340::numeric/100),2) || ', '|| ROUND((pfav_22_06_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_06_22 varchar;	
UPDATE noisemodelling.pfav SET pfav_06_22 = ROUND((pfav_06_22_020::numeric/100),2) || ', '|| ROUND((pfav_06_22_040::numeric/100),2) || ', '|| ROUND((pfav_06_22_060::numeric/100),2) || ', '|| ROUND((pfav_06_22_080::numeric/100),2) || ', '|| ROUND((pfav_06_22_100::numeric/100),2) || ', '|| ROUND((pfav_06_22_120::numeric/100),2) || ', '|| ROUND((pfav_06_22_140::numeric/100),2) || ', '|| ROUND((pfav_06_22_160::numeric/100),2) || ', '|| ROUND((pfav_06_22_180::numeric/100),2) || ', '|| ROUND((pfav_06_22_200::numeric/100),2) || ', '|| ROUND((pfav_06_22_220::numeric/100),2) || ', '|| ROUND((pfav_06_22_240::numeric/100),2) || ', '|| ROUND((pfav_06_22_260::numeric/100),2) || ', '|| ROUND((pfav_06_22_280::numeric/100),2) || ', '|| ROUND((pfav_06_22_300::numeric/100),2) || ', '|| ROUND((pfav_06_22_320::numeric/100),2) || ', '|| ROUND((pfav_06_22_340::numeric/100),2) || ', '|| ROUND((pfav_06_22_360::numeric/100),2);



-- Join Stations and their PFAV values
DROP TABLE IF EXISTS noisemodelling.station_pfav;
CREATE TABLE  noisemodelling.station_pfav AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;


DROP TABLE IF EXISTS noisemodelling.station, noisemodelling.pfav;


-- In the C_METEO_S table
/*
ALTER TABLE echeance4."C_METEO_S" 
	ADD COLUMN temp_d integer DEFAULT 15,
	ADD COLUMN temp_e integer DEFAULT 10,
	ADD COLUMN temp_n integer DEFAULT 5,
	ADD COLUMN hygro_d integer DEFAULT 70, 
	ADD COLUMN hygro_e integer DEFAULT 70,
	ADD COLUMN hygro_n integer DEFAULT 70,
	ADD COLUMN wall_alpha integer DEFAULT 0.1,
	ADD COLUMN ts_stud integer DEFAULT 0,
	ADD COLUMN pm_stud integer DEFAULT 0;
*/

