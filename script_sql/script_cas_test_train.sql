------------------------------------------------------------------
-- Script de préparation des données pour le cas test du train
-- Auteur : G. Petit
-- 02/2022
------------------------------------------------------------------


--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
-- Version avec la voie provenant d'OSM
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/

-- Mise en base de la couche RAIL_SECTIONS

DROP TABLE IF EXISTS RAIL_SECTIONS_TEST;

CALL SHPRead('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Rail_Section2.shp', 'RAIL_SECTIONS_TEST');
-- Suppression de la colonne PK2 issue de l'import
ALTER TABLE RAIL_SECTIONS_TEST DROP COLUMN PK2;
-- Mise à jour de l'altitude de la voie (fixée à 0.5m)
UPDATE RAIL_SECTIONS_TEST SET THE_GEOM=ST_AddZ(THE_GEOM, 0.5);
-- Ajout d'une largeur d'emprise, fixée à 10m
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN LARGEMPRIS INT;
UPDATE RAIL_SECTIONS_TEST SET LARGEMPRIS = 10;

SELECT * FROM RAIL_SECTIONS_TEST ;

-- Génération d'une zone de travail de 2km autour de la voie et création d'une grille régulière de 25m sur cette base. L'altitude des points de la grille est fixée à 0m
DROP TABLE IF EXISTS RAIL_BUFFER, DEM;
CREATE TABLE RAIL_BUFFER AS SELECT ST_Buffer(THE_GEOM, 2000) as THE_GEOM FROM RAIL_SECTIONS_TEST;
CREATE TABLE DEM AS SELECT ST_Force3D(ST_Centroid(THE_GEOM)) as THE_GEOM FROM ST_MakeGrid('RAIL_BUFFER', 25, 25);
ALTER TABLE DEM ADD COLUMN SOURCE VARCHAR;
UPDATE DEM SET SOURCE = 'DEM';
DROP TABLE IF EXISTS RAIL_BUFFER;


DROP TABLE IF EXISTS BDTOPO_RAIL;

CREATE TABLE BDTOPO_RAIL AS SELECT a.THE_GEOM, a.LARGEMPRIS, b.* FROM RAIL_SECTIONS_TEST a, PLATEFORM b WHERE b.IDPLATFORM ='SNCF';
CREATE SPATIAL INDEX ON BDTOPO_RAIL(the_geom);
ALTER TABLE BDTOPO_RAIL ADD pk_line INT AUTO_INCREMENT NOT NULL;
ALTER TABLE BDTOPO_RAIL add primary key(pk_line);


-- Remove DEM points that are less than "LARGEMPRIS/2" far from rails
DELETE FROM DEM WHERE EXISTS (SELECT 1 FROM BDTOPO_RAIL b WHERE ST_EXPAND(DEM.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM.THE_GEOM, b.the_geom)< ((b.LARGEMPRIS/2) + 5)  LIMIT 1) ;
    
-- Create buffer points from rails and copy the elevation from the rails to the point
DROP TABLE IF EXISTS BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;
-- The buffer size correspond to 
-- d2 = (LARGEMPRIS - 5.5)/2
CREATE TABLE BUFFERED_D2 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 5.5)/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D2') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;
    
-- d3 = (LARGEMPRIS - 4)/2
CREATE TABLE BUFFERED_D3 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 4)/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D3') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;

-- d4 = (LARGEMPRIS)/2
CREATE TABLE BUFFERED_D4 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), LARGEMPRIS/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D4') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;

 
CREATE SPATIAL INDEX ON DEM (THE_GEOM);

-- Suppression des tables inutiles
DROP TABLE IF EXISTS BDTOPO_RAIL, BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;



call SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/DEM2.shp', 'DEM');


----------------------------------------------
-- Landcover

DROP TABLE IF EXISTS LANDCOVER_CAMPAGNE, LANDCOVER_BETON;

CREATE TABLE LANDCOVER_CAMPAGNE AS SELECT ST_Envelope(ST_Buffer(THE_GEOM, 2000)) as THE_GEOM, 1.0 as g FROM RAIL_SECTIONS_TEST;
CREATE TABLE LANDCOVER_BETON AS SELECT ST_Envelope(ST_Buffer(THE_GEOM, 2000)) as THE_GEOM, 0.0 as g FROM RAIL_SECTIONS_TEST;


-- Génération des buffers autour de la voie
DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4;
-- d1 = 1.435
CREATE TABLE rail_buff_d1 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, 1.435/2))) as the_geom FROM RAIL_SECTIONS_TEST ;
-- d3 (= LARGEMPRIS-4)
CREATE TABLE rail_buff_d3 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, (LARGEMPRIS-4)/2))) as the_geom FROM RAIL_SECTIONS_TEST;
-- d4 = (LARGEMPRIS)/2
CREATE TABLE rail_buff_d4 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, LARGEMPRIS/2))) as the_geom FROM RAIL_SECTIONS_TEST;


-- Découpage des buffers
DROP TABLE IF EXISTS rail_diff_d3_d1, rail_diff_d4_d3;
CREATE TABLE rail_diff_d3_d1 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d3 a, rail_buff_d1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
CREATE TABLE rail_diff_d4_d3 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d4 a, rail_buff_d3 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

DROP TABLE IF EXISTS rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl;
CREATE TABLE rail_buff_d1_expl AS SELECT a.the_geom, b.g3 as g FROM ST_Explode('RAIL_BUFF_D1') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
CREATE TABLE rail_buff_d3_expl AS SELECT a.the_geom, b.g2 as g FROM ST_Explode('RAIL_DIFF_D3_D1 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
CREATE TABLE rail_buff_d4_expl AS SELECT a.the_geom, b.g1 as g FROM ST_Explode('RAIL_DIFF_D4_D3 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';


DROP TABLE IF EXISTS LANDCOVER_G_0, LANDCOVER_G_1;
CREATE TABLE LANDCOVER_G_0 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER_BETON WHERE g=0;
CREATE TABLE LANDCOVER_G_1 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER_CAMPAGNE WHERE g=1;

DROP TABLE IF EXISTS LANDCOVER_0_DIFF_D4, LANDCOVER_1_DIFF_D4;
CREATE TABLE LANDCOVER_0_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_0 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
CREATE TABLE LANDCOVER_1_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

DROP TABLE IF EXISTS LANDCOVER_0_EXPL, LANDCOVER_1_EXPL;
CREATE TABLE LANDCOVER_0_EXPL AS SELECT the_geom, 0 as g FROM ST_Explode('LANDCOVER_0_DIFF_D4 ');
CREATE TABLE LANDCOVER_1_EXPL AS SELECT the_geom, 1 as g FROM ST_Explode('LANDCOVER_1_DIFF_D4 ');

-- Unifiy tables
DROP TABLE IF EXISTS LANDCOVER_UNION_BETON, LANDCOVER_UNION_CAMPAGNE, LANDCOVER_MERGE_BETON, LANDCOVER_MERGE_CAMPAGNE, LANDCOVER_G0, LANDCOVER_G1;

CREATE TABLE LANDCOVER_UNION_BETON AS SELECT * FROM LANDCOVER_0_EXPL UNION SELECT * FROM RAIL_BUFF_D1_EXPL UNION SELECT * FROM RAIL_BUFF_D3_EXPL UNION SELECT * FROM RAIL_BUFF_D4_EXPL ; 
CREATE TABLE LANDCOVER_UNION_CAMPAGNE AS SELECT * FROM LANDCOVER_1_EXPL UNION SELECT * FROM RAIL_BUFF_D1_EXPL UNION SELECT * FROM RAIL_BUFF_D3_EXPL UNION SELECT * FROM RAIL_BUFF_D4_EXPL ; 

-- Merge geometries that have the same G

CREATE TABLE LANDCOVER_MERGE_BETON AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, g FROM LANDCOVER_UNION_BETON GROUP BY g;
CREATE TABLE LANDCOVER_G0 AS SELECT ST_SETSRID(the_geom,2154) as the_geom, g FROM ST_Explode('LANDCOVER_MERGE_BETON ');

CREATE TABLE LANDCOVER_MERGE_CAMPAGNE AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, g FROM LANDCOVER_UNION_CAMPAGNE GROUP BY g;
CREATE TABLE LANDCOVER_G1 AS SELECT ST_SETSRID(the_geom,2154) as the_geom, g FROM ST_Explode('LANDCOVER_MERGE_CAMPAGNE ');

DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4, rail_diff_d3_d1, rail_diff_d4_d3, rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl, LANDCOVER_G_0, LANDCOVER_G_03, LANDCOVER_G_07, LANDCOVER_G_1, LANDCOVER_0_DIFF_D4, LANDCOVER_03_DIFF_D4, LANDCOVER_07_DIFF_D4, LANDCOVER_1_DIFF_D4, LANDCOVER_0_EXPL, LANDCOVER_03_EXPL, LANDCOVER_07_EXPL, LANDCOVER_1_EXPL, LANDCOVER_UNION, LANDCOVER_MERGE, landcover_source;
DROP TABLE IF EXISTS LANDCOVER_UNION_BETON, LANDCOVER_UNION_CAMPAGNE, LANDCOVER_MERGE_BETON, LANDCOVER_MERGE_CAMPAGNE;


CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/LANDCOVER_G0.shp', 'LANDCOVER_G0');
CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/LANDCOVER_G1.shp', 'LANDCOVER_G1');


------------------------------------------------
-- Recepteurs

DROP TABLE IF EXISTS CENTRE, RINGS, RINGS_POINTS, RINGS_POINTS_EXPL, RECEPTEURS;

-- Détermination du centre de la voie ferrée
DROP TABLE IF EXISTS CENTRE;
CREATE TABLE CENTRE AS SELECT ST_MakePoint( 
	(ST_X(ST_StartPoint(the_geom))+(ST_X(ST_EndPoint(the_geom))-ST_X(ST_StartPoint(the_geom)))/2), 
	(ST_Y(ST_StartPoint(the_geom))+(ST_Y(ST_EndPoint(the_geom))-ST_Y(ST_StartPoint(the_geom)))/2)
	) as the_geom FROM RAIL_SECTIONS_TEST;

-- Génération des ring buffers
DROP TABLE IF EXISTS BUFF20, BUFF50, BUFF100, BUFF200, BUFF500;
CREATE TABLE BUFF20 AS SELECT ST_Buffer(the_geom, 20) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF50 AS SELECT ST_Buffer(the_geom, 50) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF100 AS SELECT ST_Buffer(the_geom, 100) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF200 AS SELECT ST_Buffer(the_geom, 200) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF500 AS SELECT ST_Buffer(the_geom, 500) as THE_GEOM FROM CENTRE;

-- Définition des buffers de différentes tailles
CREATE TABLE RINGS AS SELECT the_geom FROM BUFF20 
	UNION SELECT the_geom FROM BUFF50 
	UNION SELECT the_geom FROM BUFF100 
	UNION SELECT the_geom FROM BUFF200 
	UNION SELECT the_geom FROM BUFF500;

--CREATE TABLE RINGS AS SELECT ST_RingBuffer(the_geom, 50, 10) as the_geom FROM CENTRE;
DROP TABLE IF EXISTS BUFF20, BUFF50, BUFF100, BUFF200, BUFF500;
-- CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/RINGS.shp', 'RINGS');

-- Intersection entre les rings et la voie ferrée
CREATE TABLE RINGS_POINTS AS SELECT ST_ToMultiPoint(ST_Intersection(a.the_geom, b.the_geom)) as the_geom FROM RAIL_SECTIONS_TEST a, RINGS b;
CREATE TABLE RINGS_POINTS_EXPL AS SELECT the_geom FROM ST_Explode('RINGS_POINTS');
-- CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/RINGS_POINTS_EXPL.shp', 'RINGS_POINTS_EXPL');

-- Rotation des points pour produire les récepteurs
CREATE TABLE RECEPTEURS AS SELECT ST_SETSRID(ST_Rotate(a.the_geom, PI()/2, b.the_geom),2154) as the_geom FROM RINGS_POINTS_EXPL a, CENTRE b;
ALTER TABLE RECEPTEURS ADD COLUMN PK serial;
CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/RECEPTEURS.shp', 'RECEPTEURS');


DROP TABLE IF EXISTS CENTRE, RINGS, RINGS_POINTS, RINGS_POINTS_EXPL;




--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/
-- Version avec la voie horizontale
--*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/


DROP TABLE IF EXISTS RAIL, RAIL_SECTIONS_TEST;
CALL SHPRead('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Rail_Section2.shp', 'RAIL');
CREATE TABLE RAIL_SECTIONS_TEST AS SELECT ST_SETSRID(ST_MakeLine(
ST_MakePoint(ST_X(st_centroid(the_geom))-6000, ST_Y(st_centroid(the_geom))), ST_MakePoint(ST_X(st_centroid(the_geom))+6000, ST_Y(st_centroid(the_geom)))),2154) as the_geom from RAIL;
UPDATE RAIL_SECTIONS_TEST SET THE_GEOM=ST_AddZ(THE_GEOM, 0.5);
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN PK int default 1;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN IDSECTION int default 1;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN NTRACK int default 2;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN TRACKSPD int default 160;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN TRANSFER int default 7;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN ROUGHNESS int default 1;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN IMPACT int default 0;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN CURVATURE int default 0;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN BRIDGE int default 0;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN COMSPD int default 120;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN ISTUNNEL boolean default false;
ALTER TABLE RAIL_SECTIONS_TEST ADD COLUMN LARGEMPRIS int default 10;
DROP TABLE IF EXISTS RAIL;


CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/RAIL_SECTIONS.shp', 'RAIL_SECTIONS_TEST');


-- Génération de la protection acoustique, à 10m de la voie
DROP TABLE IF EXISTS RAIL_PROTECT;

CREATE TABLE RAIL_PROTECT AS SELECT ST_SETSRID(ST_MakeLine(
ST_MakePoint(ST_X(ST_StartPoint(the_geom)), ST_Y(ST_StartPoint(the_geom))+10), 
ST_MakePoint(ST_X(ST_EndPoint(the_geom)), ST_Y(ST_EndPoint(the_geom))+10)),2154) as the_geom from RAIL_SECTIONS_TEST ;

CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/RAIL_PROTECT.shp', 'RAIL_PROTECT');



-- Génération d'une zone de travail de 2km autour de la voie et création d'une grille régulière de 25m sur cette base. L'altitude des points de la grille est fixée à 0m
DROP TABLE IF EXISTS RAIL_BUFFER, DEM;
CREATE TABLE RAIL_BUFFER AS SELECT ST_Buffer(THE_GEOM, 2000) as THE_GEOM FROM RAIL_SECTIONS_TEST;
CREATE TABLE DEM AS SELECT ST_Force3D(ST_Centroid(THE_GEOM)) as THE_GEOM FROM ST_MakeGrid('RAIL_BUFFER', 25, 25);
ALTER TABLE DEM ADD COLUMN SOURCE VARCHAR;
UPDATE DEM SET SOURCE = 'DEM';
DROP TABLE IF EXISTS RAIL_BUFFER;


DROP TABLE IF EXISTS BDTOPO_RAIL;

CREATE TABLE BDTOPO_RAIL AS SELECT a.THE_GEOM, a.LARGEMPRIS, b.* FROM RAIL_SECTIONS_TEST a, PLATEFORM b WHERE b.IDPLATFORM ='SNCF';
CREATE SPATIAL INDEX ON BDTOPO_RAIL(the_geom);
ALTER TABLE BDTOPO_RAIL ADD pk_line INT AUTO_INCREMENT NOT NULL;
ALTER TABLE BDTOPO_RAIL add primary key(pk_line);



-- Remove DEM points that are less than "LARGEMPRIS/2" far from rails
DELETE FROM DEM WHERE EXISTS (SELECT 1 FROM BDTOPO_RAIL b WHERE ST_EXPAND(DEM.THE_GEOM, 20) && b.the_geom AND ST_DISTANCE(DEM.THE_GEOM, b.the_geom)< ((b.LARGEMPRIS/2) + 5)  LIMIT 1) ;
    
-- Create buffer points from rails and copy the elevation from the rails to the point
DROP TABLE IF EXISTS BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;
-- The buffer size correspond to 
-- d2 = (LARGEMPRIS - 5.5)/2
CREATE TABLE BUFFERED_D2 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 5.5)/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D2') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;
    
-- d3 = (LARGEMPRIS - 4)/2
CREATE TABLE BUFFERED_D3 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), (LARGEMPRIS - 4)/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D3') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;

-- d4 = (LARGEMPRIS)/2
CREATE TABLE BUFFERED_D4 AS SELECT ST_ToMultiPoint(ST_Densify(ST_Buffer(ST_Simplify(the_geom, 2), LARGEMPRIS/2, 'endcap=flat join=mitre'), 5 )) the_geom, pk_line from BDTOPO_RAIL where st_length(st_simplify(the_geom, 2)) > 0 ;
INSERT INTO DEM(THE_GEOM, SOURCE) SELECT ST_MakePoint(ST_X(P.THE_GEOM), ST_Y(P.THE_GEOM), ST_Z(ST_ProjectPoint(P.THE_GEOM,L.THE_GEOM))-L.H1) THE_GEOM, 'RAI' FROM ST_EXPLODE('BUFFERED_D4') P, BDTOPO_RAIL L WHERE P.PK_LINE = L.PK_LINE;

 
CREATE SPATIAL INDEX ON DEM (THE_GEOM);

-- Suppression des tables inutiles
DROP TABLE IF EXISTS BDTOPO_RAIL, BUFFERED_D2, BUFFERED_D3, BUFFERED_D4;

call SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/DEM.shp', 'DEM');


----------------------------------------------
-- Landcover

DROP TABLE IF EXISTS LANDCOVER_CAMPAGNE, LANDCOVER_BETON;

CREATE TABLE LANDCOVER_CAMPAGNE AS SELECT ST_Envelope(ST_Buffer(THE_GEOM, 2000)) as THE_GEOM, 1.0 as g FROM RAIL_SECTIONS_TEST;
CREATE TABLE LANDCOVER_BETON AS SELECT ST_Envelope(ST_Buffer(THE_GEOM, 2000)) as THE_GEOM, 0.0 as g FROM RAIL_SECTIONS_TEST;


-- Génération des buffers autour de la voie
DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4;
-- d1 = 1.435
CREATE TABLE rail_buff_d1 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, 1.435/2))) as the_geom FROM RAIL_SECTIONS_TEST ;
-- d3 (= LARGEMPRIS-4)
CREATE TABLE rail_buff_d3 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, (LARGEMPRIS-4)/2))) as the_geom FROM RAIL_SECTIONS_TEST;
-- d4 = (LARGEMPRIS)/2
CREATE TABLE rail_buff_d4 AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, LARGEMPRIS/2))) as the_geom FROM RAIL_SECTIONS_TEST;


-- Découpage des buffers
DROP TABLE IF EXISTS rail_diff_d3_d1, rail_diff_d4_d3;
CREATE TABLE rail_diff_d3_d1 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d3 a, rail_buff_d1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
CREATE TABLE rail_diff_d4_d3 as select ST_SymDifference(a.the_geom, b.the_geom) as the_geom from rail_buff_d4 a, rail_buff_d3 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

DROP TABLE IF EXISTS rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl;
CREATE TABLE rail_buff_d1_expl AS SELECT a.the_geom, b.g3 as g FROM ST_Explode('RAIL_BUFF_D1') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
CREATE TABLE rail_buff_d3_expl AS SELECT a.the_geom, b.g2 as g FROM ST_Explode('RAIL_DIFF_D3_D1 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';
CREATE TABLE rail_buff_d4_expl AS SELECT a.the_geom, b.g1 as g FROM ST_Explode('RAIL_DIFF_D4_D3 ') a, PLATEFORM  b WHERE b.IDPLATFORM ='SNCF';


DROP TABLE IF EXISTS LANDCOVER_G_0, LANDCOVER_G_1;
CREATE TABLE LANDCOVER_G_0 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER_BETON WHERE g=0;
CREATE TABLE LANDCOVER_G_1 AS SELECT ST_Union(ST_Accum(the_geom)) as the_geom FROM LANDCOVER_CAMPAGNE WHERE g=1;

DROP TABLE IF EXISTS LANDCOVER_0_DIFF_D4, LANDCOVER_1_DIFF_D4;
CREATE TABLE LANDCOVER_0_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_0 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
CREATE TABLE LANDCOVER_1_DIFF_D4 AS SELECT ST_Difference(b.the_geom, a.the_geom) as the_geom from rail_buff_d4 a, LANDCOVER_G_1 b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);

DROP TABLE IF EXISTS LANDCOVER_0_EXPL, LANDCOVER_1_EXPL;
CREATE TABLE LANDCOVER_0_EXPL AS SELECT the_geom, 0 as g FROM ST_Explode('LANDCOVER_0_DIFF_D4 ');
CREATE TABLE LANDCOVER_1_EXPL AS SELECT the_geom, 1 as g FROM ST_Explode('LANDCOVER_1_DIFF_D4 ');

-- Unifiy tables
DROP TABLE IF EXISTS LANDCOVER_UNION_BETON, LANDCOVER_UNION_CAMPAGNE, LANDCOVER_MERGE_BETON, LANDCOVER_MERGE_CAMPAGNE, LANDCOVER_G0, LANDCOVER_G1;

CREATE TABLE LANDCOVER_UNION_BETON AS SELECT * FROM LANDCOVER_0_EXPL UNION SELECT * FROM RAIL_BUFF_D1_EXPL UNION SELECT * FROM RAIL_BUFF_D3_EXPL UNION SELECT * FROM RAIL_BUFF_D4_EXPL ; 
CREATE TABLE LANDCOVER_UNION_CAMPAGNE AS SELECT * FROM LANDCOVER_1_EXPL UNION SELECT * FROM RAIL_BUFF_D1_EXPL UNION SELECT * FROM RAIL_BUFF_D3_EXPL UNION SELECT * FROM RAIL_BUFF_D4_EXPL ; 

-- Merge geometries that have the same G

CREATE TABLE LANDCOVER_MERGE_BETON AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, g FROM LANDCOVER_UNION_BETON GROUP BY g;
CREATE TABLE LANDCOVER_G0 AS SELECT ST_SETSRID(the_geom,2154) as the_geom, g FROM ST_Explode('LANDCOVER_MERGE_BETON ');

CREATE TABLE LANDCOVER_MERGE_CAMPAGNE AS SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, g FROM LANDCOVER_UNION_CAMPAGNE GROUP BY g;
CREATE TABLE LANDCOVER_G1 AS SELECT ST_SETSRID(the_geom,2154) as the_geom, g FROM ST_Explode('LANDCOVER_MERGE_CAMPAGNE ');

DROP TABLE IF EXISTS rail_buff_d1, rail_buff_d3, rail_buff_d4, rail_diff_d3_d1, rail_diff_d4_d3, rail_buff_d1_expl, rail_buff_d3_expl, rail_buff_d4_expl, LANDCOVER_G_0, LANDCOVER_G_03, LANDCOVER_G_07, LANDCOVER_G_1, LANDCOVER_0_DIFF_D4, LANDCOVER_03_DIFF_D4, LANDCOVER_07_DIFF_D4, LANDCOVER_1_DIFF_D4, LANDCOVER_0_EXPL, LANDCOVER_03_EXPL, LANDCOVER_07_EXPL, LANDCOVER_1_EXPL, LANDCOVER_UNION, LANDCOVER_MERGE, landcover_source;
DROP TABLE IF EXISTS LANDCOVER_UNION_BETON, LANDCOVER_UNION_CAMPAGNE, LANDCOVER_MERGE_BETON, LANDCOVER_MERGE_CAMPAGNE;


CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/LANDCOVER_G0.shp', 'LANDCOVER_G0');
CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/LANDCOVER_G1.shp', 'LANDCOVER_G1');


----------------------------------------------
-- Recepteurs

DROP TABLE IF EXISTS CENTRE, RINGS, RINGS_POINTS, RINGS_POINTS_EXPL, RECEPTEURS;

-- Détermination du centre de la voie ferrée
DROP TABLE IF EXISTS CENTRE;
CREATE TABLE CENTRE AS SELECT ST_MakePoint( 
	(ST_X(ST_StartPoint(the_geom))+(ST_X(ST_EndPoint(the_geom))-ST_X(ST_StartPoint(the_geom)))/2), 
	(ST_Y(ST_StartPoint(the_geom))+(ST_Y(ST_EndPoint(the_geom))-ST_Y(ST_StartPoint(the_geom)))/2)
	) as the_geom FROM RAIL_SECTIONS_TEST;

-- Génération des ring buffers
DROP TABLE IF EXISTS BUFF5, BUFF20, BUFF50, BUFF100, BUFF200, BUFF500;
CREATE TABLE BUFF5 AS SELECT ST_Buffer(the_geom, 5) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF20 AS SELECT ST_Buffer(the_geom, 20) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF50 AS SELECT ST_Buffer(the_geom, 50) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF100 AS SELECT ST_Buffer(the_geom, 100) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF200 AS SELECT ST_Buffer(the_geom, 200) as THE_GEOM FROM CENTRE;
CREATE TABLE BUFF500 AS SELECT ST_Buffer(the_geom, 500) as THE_GEOM FROM CENTRE;

-- Définition des buffers de différentes tailles
CREATE TABLE RINGS AS SELECT the_geom FROM BUFF5 
	UNION SELECT the_geom FROM BUFF20
	UNION SELECT the_geom FROM BUFF50 
	UNION SELECT the_geom FROM BUFF100 
	UNION SELECT the_geom FROM BUFF200 
	UNION SELECT the_geom FROM BUFF500;

--CREATE TABLE RINGS AS SELECT ST_RingBuffer(the_geom, 50, 10) as the_geom FROM CENTRE;
DROP TABLE IF EXISTS BUFF5, BUFF20, BUFF50, BUFF100, BUFF200, BUFF500;

-- Intersection entre les rings et la voie ferrée
CREATE TABLE RINGS_POINTS AS SELECT ST_ToMultiPoint(ST_Intersection(a.the_geom, b.the_geom)) as the_geom FROM RAIL_SECTIONS_TEST a, RINGS b;
CREATE TABLE RINGS_POINTS_EXPL AS SELECT the_geom FROM ST_Explode('RINGS_POINTS');
-- CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/RINGS_POINTS_EXPL.shp', 'RINGS_POINTS_EXPL');

-- Rotation des points pour produire les récepteurs
CREATE TABLE RECEPTEURS AS SELECT ST_SETSRID(ST_Rotate(a.the_geom, PI()/2, b.the_geom),2154) as the_geom FROM RINGS_POINTS_EXPL a, CENTRE b;
ALTER TABLE RECEPTEURS ADD COLUMN PK serial;
CALL SHPWrite('/home/gpetit/Nextcloud/Projets_de_recherche/En_cours/2020_PLAMADE/Train/PropaRail/Data_test_train_horizontal/RECEPTEURS.shp', 'RECEPTEURS');


DROP TABLE IF EXISTS CENTRE, RINGS, RINGS_POINTS, RINGS_POINTS_EXPL;