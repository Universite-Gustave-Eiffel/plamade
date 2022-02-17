--------------------------------------------------------------------------------------------
-- Script SQL pour restreindre les données d'entrée PlaMADE à une zone circulaire
--
-- Auteur : G. Petit
--
-- Ce script est à utiliser une fois que le Script "S1_Extract_Department.groovy" a été exécuté
--------------------------------------------------------------------------------------------

-- Définir les coordonnées du centre
-- Ainsi que le rayon du buffer
DROP TABLE IF EXISTS centre;
CREATE TABLE centre AS SELECT st_buffer(ST_GeomFromText('POINT(1033652.38 6291526.85)'), 2000) as the_geom FROM CONF limit 1;
CREATE SPATIAL INDEX ON centre(the_geom);

-- Filtrage des données sur la zone d'étude
DROP TABLE IF EXISTS landcover_study, roads_study, rail_sections_study, buildings_screens_study, dem_study;
CREATE TABLE landcover_study AS SELECT a.* FROM LANDCOVER a, CENTRE b WHERE a.the_geom && b.the_geom and ST_Intersects(a.the_geom, b.the_geom);
CREATE TABLE roads_study AS SELECT a.* FROM ROADS a, CENTRE b WHERE a.the_geom && b.the_geom and ST_Intersects(a.the_geom, b.the_geom);
CREATE TABLE rail_sections_study AS SELECT a.* FROM RAIL_SECTIONS  a, CENTRE b WHERE a.the_geom && b.the_geom and ST_Intersects(a.the_geom, b.the_geom);
CREATE TABLE buildings_screens_study AS SELECT a.* FROM BUILDINGS_SCREENS  a, CENTRE b WHERE a.the_geom && b.the_geom and ST_Intersects(a.the_geom, b.the_geom);
CREATE TABLE dem_study AS SELECT a.* FROM DEM  a, CENTRE b WHERE a.the_geom && b.the_geom and ST_Intersects(a.the_geom, b.the_geom);

-- Remplacement des tables existantes
DROP TABLE LANDCOVER, ROADS, RAIL_SECTIONS, BUILDINGS_SCREENS, DEM;
ALTER TABLE landcover_study RENAME TO LANDCOVER;
ALTER TABLE roads_study RENAME TO ROADS;
ALTER TABLE rail_sections_study RENAME TO RAIL_SECTIONS;
ALTER TABLE buildings_screens_study RENAME TO BUILDINGS_SCREENS;
ALTER TABLE dem_study RENAME TO DEM;

-- Ajout des contraintes nécessaires pour la suite
ALTER TABLE BUILDINGS_SCREENS ALTER COLUMN PK SET NOT NULL;
ALTER TABLE BUILDINGS_SCREENS ADD PRIMARY KEY(pk);
ALTER TABLE ROADS ALTER COLUMN PK SET NOT NULL;
ALTER TABLE ROADS ADD PRIMARY KEY(pk);
UPDATE DEM SET the_geom = ST_SetSRID(the_geom, 2154);
ALTER TABLE RAIL_SECTIONS DROP COLUMN pk;

-- Export en shp
CALL SHPWrite('/home/gpetit/06/light/DEM.shp', 'DEM');
CALL SHPWrite('/home/gpetit/06/light/LANDCOVER.shp', 'LANDCOVER');
CALL SHPWrite('/home/gpetit/06/light/ROADS.shp', 'ROADS');
CALL SHPWrite('/home/gpetit/06/light/RAIL_SECTIONS.shp', 'RAIL_SECTIONS');
CALL SHPWrite('/home/gpetit/06/light/BUILDINGS_SCREENS.shp', 'BUILDINGS_SCREENS');

DROP TABLE centre;