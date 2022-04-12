------------------------------------------------------------------------------------
-- Script d'alimentation de NoiseModelling à partir de la base de données Plamade --
-- Auteur : Gwendall Petit (Lab-STICC - CNRS UMR 6285)                            --
-- Dernière mise à jour : 02/2022                                                 --
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
-- 2- Prepare department layers, depending on projection systems
---------------------------------------------------------------------------------

-- For metropole
DROP TABLE IF EXISTS noisemodelling.departement_2154;
CREATE TABLE noisemodelling.departement_2154 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, id, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.departement_4326
	WHERE insee_dep='01' or insee_dep='02' or insee_dep='03' or insee_dep='04' or insee_dep='05' or insee_dep='06' 
	or insee_dep='07' or insee_dep='08' or insee_dep='09' or insee_dep='10' or insee_dep='11' or insee_dep='12' 
	or insee_dep='13' or insee_dep='14' or insee_dep='15' or insee_dep='16' or insee_dep='17' or insee_dep='18' 
	or insee_dep='19' or insee_dep='21' or insee_dep='22' or insee_dep='23' or insee_dep='24' or insee_dep='25' 
	or insee_dep='26' or insee_dep='27' or insee_dep='28' or insee_dep='29' or insee_dep='2A' or insee_dep='2B' 
	or insee_dep='30' or insee_dep='31' or insee_dep='32' or insee_dep='33' or insee_dep='34' or insee_dep='35' 
	or insee_dep='36' or insee_dep='37' or insee_dep='38' or insee_dep='39' or insee_dep='40' or insee_dep='41' 
	or insee_dep='42' or insee_dep='43' or insee_dep='44' or insee_dep='45' or insee_dep='46' or insee_dep='47' 
	or insee_dep='48' or insee_dep='49' or insee_dep='50' or insee_dep='51' or insee_dep='52' or insee_dep='53' 
	or insee_dep='54' or insee_dep='55' or insee_dep='56' or insee_dep='57' or insee_dep='58' or insee_dep='59' 
	or insee_dep='60' or insee_dep='61' or insee_dep='62' or insee_dep='63' or insee_dep='64' or insee_dep='65' 
	or insee_dep='66' or insee_dep='67' or insee_dep='68' or insee_dep='69' or insee_dep='70' or insee_dep='71' 
	or insee_dep='72' or insee_dep='73' or insee_dep='74' or insee_dep='75' or insee_dep='76' or insee_dep='77' 
	or insee_dep='78' or insee_dep='79' or insee_dep='80' or insee_dep='81' or insee_dep='82' or insee_dep='83' 
	or insee_dep='84' or insee_dep='85' or insee_dep='86' or insee_dep='87' or insee_dep='88' or insee_dep='89' 
	or insee_dep='90' or insee_dep='91' or insee_dep='92' or insee_dep='93' or insee_dep='94' or insee_dep='95';

CREATE INDEX departement_2154_geom_idx ON noisemodelling.departement_2154 USING gist (the_geom);
CREATE INDEX departement_2154_insee_dep_idx ON noisemodelling.departement_2154 USING btree (insee_dep);

-- For Guadeloupe and Martinique
DROP TABLE IF EXISTS noisemodelling.departement_5490;
CREATE TABLE noisemodelling.departement_5490 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, id, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.departement_4326
	WHERE insee_dep='971' or insee_dep='972';

CREATE INDEX departement_5490_geom_idx ON noisemodelling.departement_5490 USING gist (the_geom);
CREATE INDEX departement_5490_insee_dep_idx ON noisemodelling.departement_5490 USING btree (insee_dep);

-- For Guyane
DROP TABLE IF EXISTS noisemodelling.departement_2972;
CREATE TABLE noisemodelling.departement_2972 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, id, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.departement_4326
	WHERE insee_dep='973';

CREATE INDEX departement_2972_geom_idx ON noisemodelling.departement_2972 USING gist (the_geom);
CREATE INDEX departement_2972_insee_dep_idx ON noisemodelling.departement_2972 USING btree (insee_dep);

-- For La Réunion
DROP TABLE IF EXISTS noisemodelling.departement_2975;
CREATE TABLE noisemodelling.departement_2975 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, id, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.departement_4326
	WHERE insee_dep='974';

CREATE INDEX departement_2975_geom_idx ON noisemodelling.departement_2975 USING gist (the_geom);
CREATE INDEX departement_2975_insee_dep_idx ON noisemodelling.departement_2975 USING btree (insee_dep);

-- For Mayotte
DROP TABLE IF EXISTS noisemodelling.departement_4471;
CREATE TABLE noisemodelling.departement_4471 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, id, nom_dep, insee_dep, insee_reg 
	FROM noisemodelling.departement_4326
	WHERE insee_dep='976';

CREATE INDEX departement_4471_geom_idx ON noisemodelling.departement_4471 USING gist (the_geom);
CREATE INDEX departement_4471_insee_dep_idx ON noisemodelling.departement_4471 USING btree (insee_dep);

---------------------------------------------------------------------------------
-- 2- Reproject Cerema tables into the differents projection systems
---------------------------------------------------------------------------------

--------------------------------
-- For roads
--------------------------------
DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_2154";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT", "AGGLO" 
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" = '973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX n_routier_troncon_l_2154_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2154" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_2154_cbs_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2154" USING btree ("CBS_GITT");
CREATE INDEX n_routier_troncon_l_2154_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2154" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_2972";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT", "AGGLO" 
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where "CODEDEPT"='973';

CREATE INDEX n_routier_troncon_l_2972_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2972" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_2972_cbs_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2972" USING btree ("CBS_GITT");
CREATE INDEX n_routier_troncon_l_2972_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2972" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_2975";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT", "AGGLO" 
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where "CODEDEPT"='974';

CREATE INDEX n_routier_troncon_l_2975_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2975" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_2975_cbs_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2975" USING btree ("CBS_GITT");
CREATE INDEX n_routier_troncon_l_2975_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_2975" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_4471";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT", "AGGLO" 
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where "CODEDEPT"='976';

CREATE INDEX n_routier_troncon_l_4471_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_4471" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_4471_cbs_idx ON noisemodelling."N_ROUTIER_TRONCON_L_4471" USING btree ("CBS_GITT");
CREATE INDEX n_routier_troncon_l_4471_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_4471" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_TRONCON_L_5490";
CREATE TABLE noisemodelling."N_ROUTIER_TRONCON_L_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, "IDTRONCON",
	"ANNEE", "CODEDEPT", "REFPROD", "HOMOGENE", "REFGEST", "IDROUTE", "NOMRUEG", "NOMRUED",
	"INSEECOMG", "INSEECOMD", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "PRDEB", "PRFIN",
	"ZDEB", "ZFIN", "SENS", "LARGEUR", "NB_VOIES", "REPARTITIO", "FRANCHISST", "VALIDEDEB",
	"VALIDEFIN", "CBS_GITT", "AGGLO" 
	FROM echeance4."N_ROUTIER_TRONCON_L" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';

CREATE INDEX n_routier_troncon_l_5490_idroute_idx ON noisemodelling."N_ROUTIER_TRONCON_L_5490" USING btree ("IDROUTE");
CREATE INDEX n_routier_troncon_l_5490_cbs_idx ON noisemodelling."N_ROUTIER_TRONCON_L_5490" USING btree ("CBS_GITT");
CREATE INDEX n_routier_troncon_l_5490_geom_idx ON noisemodelling."N_ROUTIER_TRONCON_L_5490" USING gist (the_geom);


--------------------------------
-- For roads protections
--------------------------------

DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154";
CREATE TABLE noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDROUTE", "NOMROUTE", "INSEECOMD", "INSEECOMF", "REFSOURCE", "MILLSOURCE", 
	"IDSOURCE", "TYPEPROT", "NOMPROT", "PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", 
	"MATERIAU2", "ACCESSOIRE", "VEGETALISE", "INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_ROUTIER_PROTECTION_ACOUSTIQUE_L" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" = '973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX n_routier_protection_acoustique_l_2154_idbat_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154" USING btree ("IDPROTACOU");
CREATE INDEX n_routier_protection_acoustique_l_2154_geom_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2154" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2972";
CREATE TABLE noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDROUTE", "NOMROUTE", "INSEECOMD", "INSEECOMF", "REFSOURCE", "MILLSOURCE", 
	"IDSOURCE", "TYPEPROT", "NOMPROT", "PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", 
	"MATERIAU2", "ACCESSOIRE", "VEGETALISE", "INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_ROUTIER_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='973';

CREATE INDEX n_routier_protection_acoustique_l_2972_idbat_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2972" USING btree ("IDPROTACOU");
CREATE INDEX n_routier_protection_acoustique_l_2972_geom_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2972" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2975";
CREATE TABLE noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDROUTE", "NOMROUTE", "INSEECOMD", "INSEECOMF", "REFSOURCE", "MILLSOURCE", 
	"IDSOURCE", "TYPEPROT", "NOMPROT", "PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", 
	"MATERIAU2", "ACCESSOIRE", "VEGETALISE", "INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_ROUTIER_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='974';

CREATE INDEX n_routier_protection_acoustique_l_2975_idbat_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2975" USING btree ("IDPROTACOU");
CREATE INDEX n_routier_protection_acoustique_l_2975_geom_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_2975" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_4471";
CREATE TABLE noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDROUTE", "NOMROUTE", "INSEECOMD", "INSEECOMF", "REFSOURCE", "MILLSOURCE", 
	"IDSOURCE", "TYPEPROT", "NOMPROT", "PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", 
	"MATERIAU2", "ACCESSOIRE", "VEGETALISE", "INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_ROUTIER_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='976';

CREATE INDEX n_routier_protection_acoustique_l_4471_idbat_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_4471" USING btree ("IDPROTACOU");
CREATE INDEX n_routier_protection_acoustique_l_4471_geom_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_4471" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_5490";
CREATE TABLE noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDROUTE", "NOMROUTE", "INSEECOMD", "INSEECOMF", "REFSOURCE", "MILLSOURCE", 
	"IDSOURCE", "TYPEPROT", "NOMPROT", "PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", 
	"MATERIAU2", "ACCESSOIRE", "VEGETALISE", "INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_ROUTIER_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';

CREATE INDEX n_routier_protection_acoustique_l_5490_idbat_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_5490" USING btree ("IDPROTACOU");
CREATE INDEX n_routier_protection_acoustique_l_5490_geom_idx ON noisemodelling."N_ROUTIER_PROTECTION_ACOUSTIQUE_L_5490" USING gist (the_geom);


--------------------------------
-- For buildings
--------------------------------
DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_2154";
CREATE TABLE noisemodelling."C_BATIMENT_S_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" = '973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX c_batiment_s_2154_idbat_idx ON noisemodelling."C_BATIMENT_S_2154" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_2154_geom_idx ON noisemodelling."C_BATIMENT_S_2154" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_2972";
CREATE TABLE noisemodelling."C_BATIMENT_S_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where "CODEDEPT"='973';

CREATE INDEX c_batiment_s_2972_idbat_idx ON noisemodelling."C_BATIMENT_S_2972" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_2972_geom_idx ON noisemodelling."C_BATIMENT_S_2972" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_2975";
CREATE TABLE noisemodelling."C_BATIMENT_S_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where "CODEDEPT"='974';

CREATE INDEX c_batiment_s_2975_idbat_idx ON noisemodelling."C_BATIMENT_S_2975" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_2975_geom_idx ON noisemodelling."C_BATIMENT_S_2975" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_4471";
CREATE TABLE noisemodelling."C_BATIMENT_S_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where "CODEDEPT"='976';

CREATE INDEX c_batiment_s_4471_idbat_idx ON noisemodelling."C_BATIMENT_S_4471" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_4471_geom_idx ON noisemodelling."C_BATIMENT_S_4471" USING gist (the_geom);


DROP TABLE IF EXISTS noisemodelling."C_BATIMENT_S_5490";
CREATE TABLE noisemodelling."C_BATIMENT_S_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, 
	"IDBAT", "ANNEE", "CODEDEPT", "REFPROD", "ORIGIN_BAT", "BAT_IDTOPO", "BAT_HAUT", 
	"BAT_NB_NIV", "BAT_NATURE", "BAT_PNB", "BAT_PPBE", "BAT_UUEID"
	FROM echeance4."C_BATIMENT_S" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';

CREATE INDEX c_batiment_s_5490_idbat_idx ON noisemodelling."C_BATIMENT_S_5490" USING btree ("IDBAT");
CREATE INDEX c_batiment_s_5490_geom_idx ON noisemodelling."C_BATIMENT_S_5490" USING gist (the_geom);


--------------------------------
-- For rails
--------------------------------
DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_TRONCON_L_2154";
CREATE TABLE noisemodelling."N_FERROVIAIRE_TRONCON_L_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, "IDTRONCON", "ANNEE", "CODEDEPT", "REFPROD", "IDO", "IDF", "HOMOGENE", 
	"IDLIGNE", "NUMLIGNE", "PRDEB", "PRFIN", "SHAPE_LENG", "LARGEMPRIS", "NB_VOIES", "RAMPE", "VMAXINFRA", "CBS_GITT", "BASEVOIE", 
	"RUGOSITE", "SEMELLE", "PROTECTSUP", "JOINTRAIL", "COURBURE"  
	FROM echeance4."N_FERROVIAIRE_TRONCON_L" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" ='973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';	

CREATE INDEX n_ferroviaire_troncon_l_2154_idtroncon_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2154" USING btree ("IDTRONCON");
CREATE INDEX n_ferroviaire_troncon_l_2154_cbs_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2154" USING btree ("CBS_GITT");
CREATE INDEX n_ferroviaire_troncon_l_2154_geom_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2154" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_TRONCON_L_2972";
CREATE TABLE noisemodelling."N_FERROVIAIRE_TRONCON_L_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, "IDTRONCON", "ANNEE", "CODEDEPT", "REFPROD", "IDO", "IDF", "HOMOGENE", 
	"IDLIGNE", "NUMLIGNE", "PRDEB", "PRFIN", "SHAPE_LENG", "LARGEMPRIS", "NB_VOIES", "RAMPE", "VMAXINFRA", "CBS_GITT", "BASEVOIE", 
	"RUGOSITE", "SEMELLE", "PROTECTSUP", "JOINTRAIL", "COURBURE" 
	FROM echeance4."N_FERROVIAIRE_TRONCON_L" 
	where "CODEDEPT"='973';	

CREATE INDEX n_ferroviaire_troncon_l_2972_idtroncon_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2972" USING btree ("IDTRONCON");
CREATE INDEX n_ferroviaire_troncon_l_2972_cbs_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2972" USING btree ("CBS_GITT");
CREATE INDEX n_ferroviaire_troncon_l_2972_geom_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2972" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_TRONCON_L_2975";
CREATE TABLE noisemodelling."N_FERROVIAIRE_TRONCON_L_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, "IDTRONCON", "ANNEE", "CODEDEPT", "REFPROD", "IDO", "IDF", "HOMOGENE", 
	"IDLIGNE", "NUMLIGNE", "PRDEB", "PRFIN", "SHAPE_LENG", "LARGEMPRIS", "NB_VOIES", "RAMPE", "VMAXINFRA", "CBS_GITT", "BASEVOIE", 
	"RUGOSITE", "SEMELLE", "PROTECTSUP", "JOINTRAIL", "COURBURE" 
	FROM echeance4."N_FERROVIAIRE_TRONCON_L" 
	where "CODEDEPT"='974';	

CREATE INDEX n_ferroviaire_troncon_l_2975_idtroncon_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2975" USING btree ("IDTRONCON");
CREATE INDEX n_ferroviaire_troncon_l_2975_cbs_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2975" USING btree ("CBS_GITT");
CREATE INDEX n_ferroviaire_troncon_l_2975_geom_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_2975" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_TRONCON_L_4471";
CREATE TABLE noisemodelling."N_FERROVIAIRE_TRONCON_L_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, "IDTRONCON", "ANNEE", "CODEDEPT", "REFPROD", "IDO", "IDF", "HOMOGENE", 
	"IDLIGNE", "NUMLIGNE", "PRDEB", "PRFIN", "SHAPE_LENG", "LARGEMPRIS", "NB_VOIES", "RAMPE", "VMAXINFRA", "CBS_GITT", "BASEVOIE", 
	"RUGOSITE", "SEMELLE", "PROTECTSUP", "JOINTRAIL", "COURBURE" 
	FROM echeance4."N_FERROVIAIRE_TRONCON_L" 
	where "CODEDEPT"='976';	

CREATE INDEX n_ferroviaire_troncon_l_4471_idtroncon_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_4471" USING btree ("IDTRONCON");
CREATE INDEX n_ferroviaire_troncon_l_4471_cbs_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_4471" USING btree ("CBS_GITT");
CREATE INDEX n_ferroviaire_troncon_l_4471_geom_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_4471" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_TRONCON_L_5490";
CREATE TABLE noisemodelling."N_FERROVIAIRE_TRONCON_L_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, "IDTRONCON", "ANNEE", "CODEDEPT", "REFPROD", "IDO", "IDF", "HOMOGENE", 
	"IDLIGNE", "NUMLIGNE", "PRDEB", "PRFIN", "SHAPE_LENG", "LARGEMPRIS", "NB_VOIES", "RAMPE", "VMAXINFRA", "CBS_GITT", "BASEVOIE", 
	"RUGOSITE", "SEMELLE", "PROTECTSUP", "JOINTRAIL", "COURBURE" 
	FROM echeance4."N_FERROVIAIRE_TRONCON_L" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';	

CREATE INDEX n_ferroviaire_troncon_l_5490_idtroncon_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_5490" USING btree ("IDTRONCON");
CREATE INDEX n_ferroviaire_troncon_l_5490_cbs_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_5490" USING btree ("CBS_GITT");
CREATE INDEX n_ferroviaire_troncon_l_5490_geom_idx ON noisemodelling."N_FERROVIAIRE_TRONCON_L_5490" USING gist (the_geom);


--------------------------------
-- For rail protections
--------------------------------

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154";
CREATE TABLE noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDLIGNE", "NUMLIGNE", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "TYPEPROT", "NOMPROT",
	"PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", "MATERIAU2", "ACCESSOIRE", "VEGETALISE",
	"INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" = '973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX n_ferroviaire_protection_acoustique_l_2154_idbat_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154" USING btree ("IDPROTACOU");
CREATE INDEX n_ferroviaire_protection_acoustique_l_2154_geom_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2154" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2972";
CREATE TABLE noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDLIGNE", "NUMLIGNE", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "TYPEPROT", "NOMPROT",
	"PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", "MATERIAU2", "ACCESSOIRE", "VEGETALISE",
	"INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='973';

CREATE INDEX n_ferroviaire_protection_acoustique_l_2972_idbat_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2972" USING btree ("IDPROTACOU");
CREATE INDEX n_ferroviaire_protection_acoustique_l_2972_geom_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2972" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2975";
CREATE TABLE noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDLIGNE", "NUMLIGNE", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "TYPEPROT", "NOMPROT",
	"PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", "MATERIAU2", "ACCESSOIRE", "VEGETALISE",
	"INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='974';

CREATE INDEX n_ferroviaire_protection_acoustique_l_2975_idbat_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2975" USING btree ("IDPROTACOU");
CREATE INDEX n_ferroviaire_protection_acoustique_l_2975_geom_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_2975" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_4471";
CREATE TABLE noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDLIGNE", "NUMLIGNE", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "TYPEPROT", "NOMPROT",
	"PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", "MATERIAU2", "ACCESSOIRE", "VEGETALISE",
	"INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='976';

CREATE INDEX n_ferroviaire_protection_acoustique_l_4471_idbat_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_4471" USING btree ("IDPROTACOU");
CREATE INDEX n_ferroviaire_protection_acoustique_l_4471_geom_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_4471" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_5490";
CREATE TABLE noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, 
	"IDPROTACOU", "ANNEE", "CODEDEPT", "REFPROD", "IDLIGNE", "NUMLIGNE", "REFSOURCE", "MILLSOURCE", "IDSOURCE", "TYPEPROT", "NOMPROT",
	"PRDEB", "PRFIN", "LONGUEUR", "ZDEB", "ZFIN", "HAUTEUR", "PROPRIETE", "MATERIAU1", "MATERIAU2", "ACCESSOIRE", "VEGETALISE",
	"INCLINAISO", "SUPPORT", "VALIDEDEB", "VALIDEFIN"
	FROM echeance4."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';

CREATE INDEX n_ferroviaire_protection_acoustique_l_5490_idbat_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_5490" USING btree ("IDPROTACOU");
CREATE INDEX n_ferroviaire_protection_acoustique_l_5490_geom_idx ON noisemodelling."N_FERROVIAIRE_PROTECTION_ACOUSTIQUE_L_5490" USING gist (the_geom);


--------------------------------
-- For NATURE_SOL
--------------------------------
DROP TABLE IF EXISTS noisemodelling."C_NATURESOL_S_2154";
CREATE TABLE noisemodelling."C_NATURESOL_S_2154" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, 
	"IDNATSOL", "ANNEE", "CODEDEPT", "REFPROD", "NATSOL_CLC", "NATSOL_CNO", "NATSOL_LIB"
	FROM echeance4."C_NATURESOL_S" 
	where not "CODEDEPT"='971' and not "CODEDEPT" = '972' and not "CODEDEPT" = '973' and not "CODEDEPT" ='974' and not "CODEDEPT" = '976';

CREATE INDEX c_nature_sol_s_2154_idnasol_idx ON noisemodelling."C_NATURESOL_S_2154" USING btree ("IDNATSOL");
CREATE INDEX c_nature_sol_s_2154_natsolcno_idx ON noisemodelling."C_NATURESOL_S_2154" USING btree ("NATSOL_CNO");
CREATE INDEX c_nature_sol_s_2154_geom_idx ON noisemodelling."C_NATURESOL_S_2154" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."C_NATURESOL_S_2972";
CREATE TABLE noisemodelling."C_NATURESOL_S_2972" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2972) as the_geom, 
	"IDNATSOL", "ANNEE", "CODEDEPT", "REFPROD", "NATSOL_CLC", "NATSOL_CNO", "NATSOL_LIB"
	FROM echeance4."C_NATURESOL_S" 
	where "CODEDEPT"='973';

CREATE INDEX c_nature_sol_s_2972_idnasol_idx ON noisemodelling."C_NATURESOL_S_2972" USING btree ("IDNATSOL");
CREATE INDEX c_nature_sol_s_2972_natsolcno_idx ON noisemodelling."C_NATURESOL_S_2972" USING btree ("NATSOL_CNO");
CREATE INDEX c_nature_sol_s_2972_geom_idx ON noisemodelling."C_NATURESOL_S_2972" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."C_NATURESOL_S_2975";
CREATE TABLE noisemodelling."C_NATURESOL_S_2975" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2975) as the_geom, 
	"IDNATSOL", "ANNEE", "CODEDEPT", "REFPROD", "NATSOL_CLC", "NATSOL_CNO", "NATSOL_LIB"
	FROM echeance4."C_NATURESOL_S" 
	where "CODEDEPT"='974';

CREATE INDEX c_nature_sol_s_2975_idnasol_idx ON noisemodelling."C_NATURESOL_S_2975" USING btree ("IDNATSOL");
CREATE INDEX c_nature_sol_s_2975_natsolcno_idx ON noisemodelling."C_NATURESOL_S_2975" USING btree ("NATSOL_CNO");
CREATE INDEX c_nature_sol_s_2975_geom_idx ON noisemodelling."C_NATURESOL_S_2975" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."C_NATURESOL_S_4471";
CREATE TABLE noisemodelling."C_NATURESOL_S_4471" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 4471) as the_geom, 
	"IDNATSOL", "ANNEE", "CODEDEPT", "REFPROD", "NATSOL_CLC", "NATSOL_CNO", "NATSOL_LIB"
	FROM echeance4."C_NATURESOL_S" 
	where "CODEDEPT"='976';

CREATE INDEX c_nature_sol_s_4471_idnasol_idx ON noisemodelling."C_NATURESOL_S_4471" USING btree ("IDNATSOL");
CREATE INDEX c_nature_sol_s_4471_natsolcno_idx ON noisemodelling."C_NATURESOL_S_4471" USING btree ("NATSOL_CNO");
CREATE INDEX c_nature_sol_s_4471_geom_idx ON noisemodelling."C_NATURESOL_S_4471" USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling."C_NATURESOL_S_5490";
CREATE TABLE noisemodelling."C_NATURESOL_S_5490" AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 5490) as the_geom, 
	"IDNATSOL", "ANNEE", "CODEDEPT", "REFPROD", "NATSOL_CLC", "NATSOL_CNO", "NATSOL_LIB"
	FROM echeance4."C_NATURESOL_S" 
	where "CODEDEPT"='971' or "CODEDEPT"='972';

CREATE INDEX c_nature_sol_s_5490_idnasol_idx ON noisemodelling."C_NATURESOL_S_5490" USING btree ("IDNATSOL");
CREATE INDEX c_nature_sol_s_5490_natsolcno_idx ON noisemodelling."C_NATURESOL_S_5490" USING btree ("NATSOL_CNO");
CREATE INDEX c_nature_sol_s_5490_geom_idx ON noisemodelling."C_NATURESOL_S_5490" USING gist (the_geom);

---------------------------------------------------------------------------------
-- 3- Generate configuration and parameters tables
---------------------------------------------------------------------------------

----------------------------------
-- CONF table
DROP TABLE IF EXISTS noisemodelling.conf;
CREATE TABLE noisemodelling.conf (confId integer Primary Key, confReflOrder integer, confMaxSrcDist integer, confMaxReflDist integer, 
	confDistBuildingsReceivers integer, confThreadNumber integer, confDiffVertical boolean, confDiffHorizontal boolean, 
	confSkipLday boolean, confSkipLevening boolean, confSkipLnight boolean, confSkipLden boolean, confExportSourceId boolean, wall_alpha real);
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
COMMENT ON COLUMN noisemodelling.conf.wall_alpha IS 'Ground absorption coefficient';

-- Insert values
INSERT INTO noisemodelling.conf VALUES(1, 0, 250, 50, 5, 1, false, false, true, true, false, false, true, 0.1);
INSERT INTO noisemodelling.conf VALUES(2, 0, 250, 50, 5, 1, false, false, true, true, false, false, false, 0.1);
INSERT INTO noisemodelling.conf VALUES(3, 1, 800, 250, 5, 3, false, true, true, true, false, false, true, 0.1);
INSERT INTO noisemodelling.conf VALUES(4, 1, 800, 250, 5, 16, false, true, true, true, false, false, true, 0.1);
INSERT INTO noisemodelling.conf VALUES(5, 1, 800, 800, 5, 16, false, true, true, true, false, false, true, 0.1);
INSERT INTO noisemodelling.conf VALUES(6, 3, 800, 800, 5, 16, false, true, true, true, false, false, true, 0.1);


----------------------------------
-- CONF_ROAD table
DROP TABLE IF EXISTS noisemodelling.conf_road;
CREATE TABLE noisemodelling.conf_road (idConf integer Primary Key, junc_dist float8, junc_type integer);
COMMENT ON COLUMN noisemodelling.conf_road.idConf IS 'Configuration identifier';
COMMENT ON COLUMN noisemodelling.conf_road.junc_dist IS 'Distance to junction in meters';
COMMENT ON COLUMN noisemodelling.conf_road.junc_type IS 'Type of junction (k=0 none, k = 1 for a crossing with traffic lights, k = 2 for a roundabout)';

-- Insert values
INSERT INTO noisemodelling.conf_road VALUES(1, 200, 0);

----------------------------------
-- CONF_RAIL table
DROP TABLE IF EXISTS noisemodelling.conf_rail;
CREATE TABLE noisemodelling.conf_rail (idConf integer Primary Key, idPlatform varchar, runCdtn integer, idling float);
COMMENT ON COLUMN noisemodelling.conf_rail.idConf IS 'Configuration identifier';
COMMENT ON COLUMN noisemodelling.conf_rail.idPlatform IS 'Foreign key to the plateform table';
COMMENT ON COLUMN noisemodelling.conf_rail.runCdtn IS 'Listed code describing the running condition of the train : constant=0, accelerating=1, decelerating=2, idling=3';
COMMENT ON COLUMN noisemodelling.conf_rail.idling IS 'Idling Time (only used when RunningCondition is "idling")';


----------------------------------
-- PLATFORM table
DROP TABLE IF EXISTS noisemodelling.platform;
CREATE TABLE noisemodelling.platform (idPlatform varchar Primary Key, d1 float, g1 float, g2 float, g3 float, h1 float, h2 float);

COMMENT ON COLUMN noisemodelling.platform.idPlatform IS 'Platform id';
COMMENT ON COLUMN noisemodelling.platform.d1 IS 'Ecartement des rails (en mètre)';
COMMENT ON COLUMN noisemodelling.platform.g1 IS 'Facteur de sol de la platform';
COMMENT ON COLUMN noisemodelling.platform.g2 IS 'Facteur de sol de la banquette de ballast';
COMMENT ON COLUMN noisemodelling.platform.g3 IS 'Facteur de sol entre les rails';
COMMENT ON COLUMN noisemodelling.platform.h1 IS 'Hauteur de la banquette de ballast (en mètre)';
COMMENT ON COLUMN noisemodelling.platform.h2 IS 'Hauteur des rails libres au-dessus du ballast (en mètre)';

INSERT INTO noisemodelling.platform VALUES ('SNCF', 1.435, 0, 1, 1, 0.5, 0.18);


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
DROP TABLE IF EXISTS noisemodelling.station_2154;
CREATE TABLE noisemodelling.station_2154 (the_geom geometry (POINT, 2154), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(616373.3907217126 7001744.583853569)', 2154),'Abbeville','SURFCOMM0000000088879389','80001');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(893919.0293355002 6273725.142621572)', 2154),'Aix-en-Provence','SURFCOMM0000000041520664','13001');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(674607.5701839515 6659131.366549444)', 2154),'Avord','SURFCOMM0000000028066429','18018');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(417799.9976869353 6423990.918437644)', 2154),'Bordeaux','SURFCOMM0000000052189828','33063');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(145530.00273641292 6837477.3869183585)', 2154),'Brest','SURFCOMM0000000030001423','29019');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(454371.5118771473 6903601.663448182)', 2154),'Caen','SURFCOMM0000000028305288','14118');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(646608.1611191704 6234689.5157902)', 2154),'Carcassonne','SURFCOMM0000000082026644','11069');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(865149.1206673698 6331088.0969790565)', 2154),'Carpentras','SURFCOMM0000000039657837','84031');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(853877.7920067526 6693376.86887768)', 2154),'Dijon','SURFCOMM0000000053362260','21231');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(327178.87603466475 6848017.307727337)', 2154),'Dinard','SURFCOMM0000000049126777','35093');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(653363.0240544211 7104019.048392724)', 2154),'Dunkerque','SURFCOMM0000000256883843','59183');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(564075.9883230523 6881586.733291183)', 2154),'Évreux','SURFCOMM0000000082908132','27229');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(1004552.3764158675 6270914.394926301)', 2154),'Fréjus','SURFCOMM0000000075370462','83061');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(571502.1816703166 6405288.202799344)', 2154),'Gourdon','SURFCOMM0000000034543660','46127');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(378105.88037093764 6571019.596784672)', 2154),'La Rochelle','SURFCOMM0000000035782630','17300');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(419583.9515651179 6780101.639063162)', 2154),'Laval','SURFCOMM0000000029679566','53130');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(703330.7171756831 7059432.721058839)', 2154),'Lille','SURFCOMM0000000057564909','59350');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(564123.4842605114 6529811.830314347)', 2154),'Limoges','SURFCOMM0000000034695959','87085');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(222544.97974805092 6758220.078977364)', 2154),'Lorient','SURFCOMM0000000087808502','56121');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(951634.272585947 6752395.312970484)', 2154),'Luxeuil-les-Bains','SURFCOMM0000000048265697','70311');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(842740.5769202277 6518916.568179048)', 2154),'Lyon','SURFCOMM0000000025564961','69123');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(839914.1670596567 6581750.749848031)', 2154),'Mâcon','SURFCOMM0000000043956231','71270');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(419640.23345633235 6317366.443854319)', 2154),'Mont-de-Marsan','SURFCOMM0000000030165836','40192');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(838860.8363883786 6385394.293790727)', 2154),'Montélimar','SURFCOMM0000000026655997','26198');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(770192.1200213472 6279753.20414169)', 2154),'Montpellier','SURFCOMM0000000046595170','34172');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(933662.8902057507 6848022.89884269)', 2154),'Nancy','SURFCOMM0000000054047095','54395');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(356071.7575110152 6691211.887896891)', 2154),'Nantes','SURFCOMM0000000029999037','44109');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(1041459.8849840319 6299535.359835112)', 2154),'Nice','SURFCOMM0000000075358432','06088');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(808376.2272716055 6305993.907192477)', 2154),'Nîmes','SURFCOMM0000000036253934','30189');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(619024.1861362043 6754096.477293332)', 2154),'Orléans','SURFCOMM0000000028065065','45234');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(428173.3901694165 6252531.915747433)', 2154),'Pau','SURFCOMM0000000028107759','64445');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(691711.445832622 6177430.403179263)', 2154),'Perpignan','SURFCOMM0000000039658911','66136');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(497854.1963258736 6612669.423452804)', 2154),'Poitiers','SURFCOMM0000002001872004','86194');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(775739.6248968422 6906283.006674417)', 2154),'Reims','SURFCOMM0000000066200172','51454');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(351768.41152412945 6789360.740158863)', 2154),'Rennes','SURFCOMM0000000049126597','35238');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(843642.9525351524 6838224.707560948)', 2154),'Saint-Dizier','SURFCOMM0000000066995929','52448');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(719990.6623170109 6972100.243269524)', 2154),'Saint-Quentin','SURFCOMM0000000064722261','02691');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(1051530.0080132915 6840730.093252666)', 2154),'Strasbourg','SURFCOMM0000000051649648','67482');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(573360.7726835464 6278698.498061831)', 2154),'Toulouse','SURFCOMM0000000073650926','31555');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(526250.5969930588 6702318.560193463)', 2154),'Tours','SURFCOMM0000000035784550','37261');
INSERT INTO noisemodelling.station_2154 VALUES(ST_GeomFromText('POINT(376070.4581367768 6944038.247843531)', 2154),'Valognes','SURFCOMM0000000029538809','50615');


DROP TABLE IF EXISTS noisemodelling.station_2972;
CREATE TABLE noisemodelling.station_2972 (the_geom geometry (POINT, 2972), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station_2972 VALUES(ST_GeomFromText('POINT(251476.72182143063 433769.3140909409)', 2972),'Guyane','Guyane','973');

DROP TABLE IF EXISTS noisemodelling.station_2975;
CREATE TABLE noisemodelling.station_2975 (the_geom geometry (POINT, 2975), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station_2975 VALUES(ST_GeomFromText('POINT(347598.7498991556 7662412.132290259)', 2975),'La Réunion','La Réunion','974');

DROP TABLE IF EXISTS noisemodelling.station_4471;
CREATE TABLE noisemodelling.station_4471 (the_geom geometry (POINT, 4471), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station_4471 VALUES(ST_GeomFromText('POINT(516000.04191931343 8582699.651614746)', 4471),'Mayotte','Mayotte','976');

DROP TABLE IF EXISTS noisemodelling.station_5490;
CREATE TABLE noisemodelling.station_5490 (the_geom geometry (POINT, 5490), name varchar, id varchar Primary Key, insee_station varchar);
INSERT INTO noisemodelling.station_5490 VALUES(ST_GeomFromText('POINT(656091.8730272526 1791348.535236883)', 5490),'Guadeloupe','Guadeloupe','971');
INSERT INTO noisemodelling.station_5490 VALUES(ST_GeomFromText('POINT(713316.9527703777 1621079.7857399185)', 5490),'Martinique','Martinique','972');


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
 ('Valognes','50615',37,35,33,32,30,30,31,33,35,38,41,45,48,49,49,46,42,39,61,54,49,45,44,43,44,45,46,48,53,58,61,64,65,65,65,64,58,52,47,43,40,38,39,42,44,47,51,55,59,63,66,65,63,61,43,40,37,35,34,33,34,36,38,40,44,48,51,52,53,51,48,45),
 ('Guadeloupe','971',50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50),
 ('Martinique','972',50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50),
 ('Guyane','973',50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50),
 ('La Réunion','974',50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50),
 ('Mayotte','976',50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50,50);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_06_18 varchar;
UPDATE noisemodelling.pfav SET pfav_06_18 = ROUND((pfav_06_18_020::numeric/100),2) || ', '|| ROUND((pfav_06_18_040::numeric/100),2) || ', '|| ROUND((pfav_06_18_060::numeric/100),2) || ', '|| ROUND((pfav_06_18_080::numeric/100),2) || ', '|| ROUND((pfav_06_18_100::numeric/100),2) || ', '|| ROUND((pfav_06_18_120::numeric/100),2) || ', '|| ROUND((pfav_06_18_140::numeric/100),2) || ', '|| ROUND((pfav_06_18_160::numeric/100),2) || ', '|| ROUND((pfav_06_18_180::numeric/100),2) || ', '|| ROUND((pfav_06_18_200::numeric/100),2) || ', '|| ROUND((pfav_06_18_220::numeric/100),2) || ', '|| ROUND((pfav_06_18_240::numeric/100),2) || ', '|| ROUND((pfav_06_18_260::numeric/100),2) || ', '|| ROUND((pfav_06_18_280::numeric/100),2) || ', '|| ROUND((pfav_06_18_300::numeric/100),2) || ', '|| ROUND((pfav_06_18_320::numeric/100),2) || ', '|| ROUND((pfav_06_18_340::numeric/100),2) || ', '|| ROUND((pfav_06_18_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_18_22 varchar;
UPDATE noisemodelling.pfav SET pfav_18_22 = ROUND((pfav_18_22_020::numeric/100),2) || ', '|| ROUND((pfav_18_22_040::numeric/100),2) || ', '|| ROUND((pfav_18_22_060::numeric/100),2) || ', '|| ROUND((pfav_18_22_080::numeric/100),2) || ', '|| ROUND((pfav_18_22_100::numeric/100),2) || ', '|| ROUND((pfav_18_22_120::numeric/100),2) || ', '|| ROUND((pfav_18_22_140::numeric/100),2) || ', '|| ROUND((pfav_18_22_160::numeric/100),2) || ', '|| ROUND((pfav_18_22_180::numeric/100),2) || ', '|| ROUND((pfav_18_22_200::numeric/100),2) || ', '|| ROUND((pfav_18_22_220::numeric/100),2) || ', '|| ROUND((pfav_18_22_240::numeric/100),2) || ', '|| ROUND((pfav_18_22_260::numeric/100),2) || ', '|| ROUND((pfav_18_22_280::numeric/100),2) || ', '|| ROUND((pfav_18_22_300::numeric/100),2) || ', '|| ROUND((pfav_18_22_320::numeric/100),2) || ', '|| ROUND((pfav_18_22_340::numeric/100),2) || ', '|| ROUND((pfav_18_22_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_22_06 varchar;
UPDATE noisemodelling.pfav SET pfav_22_06 = ROUND((pfav_22_06_020::numeric/100),2) || ', '|| ROUND((pfav_22_06_040::numeric/100),2) || ', '|| ROUND((pfav_22_06_060::numeric/100),2) || ', '|| ROUND((pfav_22_06_080::numeric/100),2) || ', '|| ROUND((pfav_22_06_100::numeric/100),2) || ', '|| ROUND((pfav_22_06_120::numeric/100),2) || ', '|| ROUND((pfav_22_06_140::numeric/100),2) || ', '|| ROUND((pfav_22_06_160::numeric/100),2) || ', '|| ROUND((pfav_22_06_180::numeric/100),2) || ', '|| ROUND((pfav_22_06_200::numeric/100),2) || ', '|| ROUND((pfav_22_06_220::numeric/100),2) || ', '|| ROUND((pfav_22_06_240::numeric/100),2) || ', '|| ROUND((pfav_22_06_260::numeric/100),2) || ', '|| ROUND((pfav_22_06_280::numeric/100),2) || ', '|| ROUND((pfav_22_06_300::numeric/100),2) || ', '|| ROUND((pfav_22_06_320::numeric/100),2) || ', '|| ROUND((pfav_22_06_340::numeric/100),2) || ', '|| ROUND((pfav_22_06_360::numeric/100),2);

ALTER TABLE noisemodelling.pfav ADD COLUMN pfav_06_22 varchar;	
UPDATE noisemodelling.pfav SET pfav_06_22 = ROUND((pfav_06_22_020::numeric/100),2) || ', '|| ROUND((pfav_06_22_040::numeric/100),2) || ', '|| ROUND((pfav_06_22_060::numeric/100),2) || ', '|| ROUND((pfav_06_22_080::numeric/100),2) || ', '|| ROUND((pfav_06_22_100::numeric/100),2) || ', '|| ROUND((pfav_06_22_120::numeric/100),2) || ', '|| ROUND((pfav_06_22_140::numeric/100),2) || ', '|| ROUND((pfav_06_22_160::numeric/100),2) || ', '|| ROUND((pfav_06_22_180::numeric/100),2) || ', '|| ROUND((pfav_06_22_200::numeric/100),2) || ', '|| ROUND((pfav_06_22_220::numeric/100),2) || ', '|| ROUND((pfav_06_22_240::numeric/100),2) || ', '|| ROUND((pfav_06_22_260::numeric/100),2) || ', '|| ROUND((pfav_06_22_280::numeric/100),2) || ', '|| ROUND((pfav_06_22_300::numeric/100),2) || ', '|| ROUND((pfav_06_22_320::numeric/100),2) || ', '|| ROUND((pfav_06_22_340::numeric/100),2) || ', '|| ROUND((pfav_06_22_360::numeric/100),2);



-- Join Stations and their PFAV values
DROP TABLE IF EXISTS noisemodelling.station_pfav_2154;
CREATE TABLE  noisemodelling.station_pfav_2154 AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station_2154 a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;

DROP TABLE IF EXISTS noisemodelling.station_pfav_2972;
CREATE TABLE  noisemodelling.station_pfav_2972 AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station_2972 a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;

DROP TABLE IF EXISTS noisemodelling.station_pfav_2975;
CREATE TABLE  noisemodelling.station_pfav_2975 AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station_2975 a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;

DROP TABLE IF EXISTS noisemodelling.station_pfav_4471;
CREATE TABLE  noisemodelling.station_pfav_4471 AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station_4471 a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;

DROP TABLE IF EXISTS noisemodelling.station_pfav_5490;
CREATE TABLE  noisemodelling.station_pfav_5490 AS SELECT a.the_geom, a.id, b.* 
FROM noisemodelling.station_5490 a, noisemodelling.pfav b
WHERE a.insee_station = b.insee_station;

DROP TABLE IF EXISTS noisemodelling.station_2154, noisemodelling.station_2972, noisemodelling.station_2975, noisemodelling.station_4471, noisemodelling.station_5490, noisemodelling.pfav;


-- Generate NUTS table
DROP TABLE IF EXISTS noisemodelling.nuts;
CREATE TABLE noisemodelling.nuts(code_2021 VARCHAR(9) NOT NULL PRIMARY KEY,country VARCHAR(7), 
   nuts_lvl1 VARCHAR(46), nuts_lvl2 VARCHAR(26),nuts_lvl3 VARCHAR(23), code_dept VARCHAR(3), ratio_pop_log FLOAT);
INSERT INTO noisemodelling.nuts VALUES ('FR','France',NULL,NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FR1',NULL,'Ile-de-France',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FR10',NULL,NULL,'Ile-de-France',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FR101',NULL,NULL,NULL,'Paris', '075', 1.6);
INSERT INTO noisemodelling.nuts VALUES ('FR102',NULL,NULL,NULL,'Seine-et-Marne', '077', 2.3);
INSERT INTO noisemodelling.nuts VALUES ('FR103',NULL,NULL,NULL,'Yvelines','078', 2.3);
INSERT INTO noisemodelling.nuts VALUES ('FR104',NULL,NULL,NULL,'Essonne', '091', 2.4);
INSERT INTO noisemodelling.nuts VALUES ('FR105',NULL,NULL,NULL,'Hauts-de-Seine', '092', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FR106',NULL,NULL,NULL,'Seine-Saint-Denis', '093', 2.5);
INSERT INTO noisemodelling.nuts VALUES ('FR107',NULL,NULL,NULL,'Val-de-Marne', '094', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FR108',NULL,NULL,NULL,'Val-d''Oise', '095', 2.4);
INSERT INTO noisemodelling.nuts VALUES ('FRB',NULL,'Centre - Val de Loire',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRB0',NULL,NULL,'Centre - Val de Loire',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRB01',NULL,NULL,NULL,'Cher', '018', 1.6);
INSERT INTO noisemodelling.nuts VALUES ('FRB02',NULL,NULL,NULL,'Eure-et-Loir', '028', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRB03',NULL,NULL,NULL,'Indre', '036', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRB04',NULL,NULL,NULL,'Indre-et-Loire', '037', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRB05',NULL,NULL,NULL,'Loir-et-Cher', '041', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRB06',NULL,NULL,NULL,'Loiret', '045', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRC',NULL,'Bourgogne-Franche-Comté',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRC1',NULL,NULL,'Bourgogne',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRC11',NULL,NULL,NULL,'Côte-d''Or', '021', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRC12',NULL,NULL,NULL,'Nièvre', '058', 1.3);
INSERT INTO noisemodelling.nuts VALUES ('FRC13',NULL,NULL,NULL,'Saône-et-Loire', '071', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRC14',NULL,NULL,NULL,'Yonne', '089', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRC2',NULL,NULL,'Franche-Comté',NULL,NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRC21',NULL,NULL,NULL,'Doubs', '025', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRC22',NULL,NULL,NULL,'Jura', '039', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRC23',NULL,NULL,NULL,'Haute-Saône', '070', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRC24',NULL,NULL,NULL,'Territoire de Belfort', '090', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FRD',NULL,'Normandie',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRD1',NULL,NULL,'Basse-Normandie',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRD11',NULL,NULL,NULL,'Calvados', '014', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRD12',NULL,NULL,NULL,'Manche', '050', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRD13',NULL,NULL,NULL,'Orne', '061', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRD2',NULL,NULL,'Haute-Normandie',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRD21',NULL,NULL,NULL,'Eure', '027', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRD22',NULL,NULL,NULL,'Seine-Maritime', '076', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FRE',NULL,'Hauts-de-France',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRE1',NULL,NULL,'Nord-Pas de Calais',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRE11',NULL,NULL,NULL,'Nord', '059', 2.3);
INSERT INTO noisemodelling.nuts VALUES ('FRE12',NULL,NULL,NULL,'Pas-de-Calais', '062', 2.3);
INSERT INTO noisemodelling.nuts VALUES ('FRE2',NULL,NULL,'Picardie',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRE21',NULL,NULL,NULL,'Aisne', '002', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRE22',NULL,NULL,NULL,'Oise', '060', 2.3);
INSERT INTO noisemodelling.nuts VALUES ('FRE23',NULL,NULL,NULL,'Somme', '080', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRF',NULL,'Grand Est',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRF1',NULL,NULL,'Alsace',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRF11',NULL,NULL,NULL,'Bas-Rhin', '067', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FRF12',NULL,NULL,NULL,'Haut-Rhin', '068', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRF2',NULL,NULL,'Champagne-Ardenne',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRF21',NULL,NULL,NULL,'Ardennes', '008', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRF22',NULL,NULL,NULL,'Aube', '010', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRF23',NULL,NULL,NULL,'Marne', '051', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRF24',NULL,NULL,NULL,'Haute-Marne', '052', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRF3',NULL,NULL,'Lorraine',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRF31',NULL,NULL,NULL,'Meurthe-et-Moselle', '054', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRF32',NULL,NULL,NULL,'Meuse', '055', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRF33',NULL,NULL,NULL,'Moselle', '057', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FRF34',NULL,NULL,NULL,'Vosges', '088', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRG',NULL,'Pays de la Loire',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRG0',NULL,NULL,'Pays de la Loire',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRG01',NULL,NULL,NULL,'Loire-Atlantique', '044', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRG02',NULL,NULL,NULL,'Maine-et-Loire', '049', 2.2);
INSERT INTO noisemodelling.nuts VALUES ('FRG03',NULL,NULL,NULL,'Mayenne', '053', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRG04',NULL,NULL,NULL,'Sarthe', '072', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRG05',NULL,NULL,NULL,'Vendée', '085', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRH',NULL,'Bretagne',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRH0',NULL,NULL,'Bretagne',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRH01',NULL,NULL,NULL,'Côtes-d''Armor', '022', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRH02',NULL,NULL,NULL,'Finistère', '029', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRH03',NULL,NULL,NULL,'Ille-et-Vilaine', '035', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRH04',NULL,NULL,NULL,'Morbihan', '056', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRI',NULL,'Nouvelle-Aquitaine',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRI1',NULL,NULL,'Aquitaine',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRI11',NULL,NULL,NULL,'Dordogne', '024', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRI12',NULL,NULL,NULL,'Gironde', '033', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRI13',NULL,NULL,NULL,'Landes', '040', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRI14',NULL,NULL,NULL,'Lot-et-Garonne', '047', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRI15',NULL,NULL,NULL,'Pyrénées-Atlantiques', '064', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRI2',NULL,NULL,'Limousin',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRI21',NULL,NULL,NULL,'Corrèze', '019', 1.4);
INSERT INTO noisemodelling.nuts VALUES ('FRI22',NULL,NULL,NULL,'Creuse', '023', 1.2);
INSERT INTO noisemodelling.nuts VALUES ('FRI23',NULL,NULL,NULL,'Haute-Vienne', '087', 1.6);
INSERT INTO noisemodelling.nuts VALUES ('FRI3',NULL,NULL,'Poitou-Charentes',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRI31',NULL,NULL,NULL,'Charente', '016', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRI32',NULL,NULL,NULL,'Charente-Maritime', '017', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRI33',NULL,NULL,NULL,'Deux-Sèvres', '079', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRI34',NULL,NULL,NULL,'Vienne', '086', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRJ',NULL,'Occitanie',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRJ1',NULL,NULL,'Languedoc-Roussillon',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRJ11',NULL,NULL,NULL,'Aude', '011', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRJ12',NULL,NULL,NULL,'Gard', '030', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRJ13',NULL,NULL,NULL,'Hérault', '034', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRJ14',NULL,NULL,NULL,'Lozère', '048', 1.1);
INSERT INTO noisemodelling.nuts VALUES ('FRJ15',NULL,NULL,NULL,'Pyrénées-Orientales', '066', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRJ2',NULL,NULL,'Midi-Pyrénées',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRJ21',NULL,NULL,NULL,'Ariège', '009', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRJ22',NULL,NULL,NULL,'Aveyron', '012', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRJ23',NULL,NULL,NULL,'Haute-Garonne', '031', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRJ24',NULL,NULL,NULL,'Gers', '032', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRJ25',NULL,NULL,NULL,'Lot', '046', 1.4);
INSERT INTO noisemodelling.nuts VALUES ('FRJ26',NULL,NULL,NULL,'Hautes-Pyrénées', '065', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRJ27',NULL,NULL,NULL,'Tarn', '081', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRJ28',NULL,NULL,NULL,'Tarn-et-Garonne', '082', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRK',NULL,'Auvergne-Rhône-Alpes',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRK1',NULL,NULL,'Auvergne',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRK11',NULL,NULL,NULL,'Allier', '003', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRK12',NULL,NULL,NULL,'Cantal', '015', 1.3);
INSERT INTO noisemodelling.nuts VALUES ('FRK13',NULL,NULL,NULL,'Haute-Loire', '043', 1.3);
INSERT INTO noisemodelling.nuts VALUES ('FRK14',NULL,NULL,NULL,'Puy-de-Dôme', '063', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRK2',NULL,NULL,'Rhône-Alpes',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRK21',NULL,NULL,NULL,'Ain', '001', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRK22',NULL,NULL,NULL,'Ardèche', '007', 1.4);
INSERT INTO noisemodelling.nuts VALUES ('FRK23',NULL,NULL,NULL,'Drôme', '026', 1.7);
INSERT INTO noisemodelling.nuts VALUES ('FRK24',NULL,NULL,NULL,'Isère', '038', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRK25',NULL,NULL,NULL,'Loire', '042', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRK26',NULL,NULL,NULL,'Rhône', '069', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRK27',NULL,NULL,NULL,'Savoie', '073', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRK28',NULL,NULL,NULL,'Haute-Savoie', '074', 2);
INSERT INTO noisemodelling.nuts VALUES ('FRL',NULL,'Provence-Alpes-Côte d''Azur',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRL0',NULL,NULL,'Provence-Alpes-Côte d''Azur',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRL01',NULL,NULL,NULL,'Alpes-de-Haute-Provence', '004', 1.2);
INSERT INTO noisemodelling.nuts VALUES ('FRL02',NULL,NULL,NULL,'Hautes-Alpes', '005', 1.2);
INSERT INTO noisemodelling.nuts VALUES ('FRL03',NULL,NULL,NULL,'Alpes-Maritimes', '006', 1.5);
INSERT INTO noisemodelling.nuts VALUES ('FRL04',NULL,NULL,NULL,'Bouches-du-Rhône', '013', 2.1);
INSERT INTO noisemodelling.nuts VALUES ('FRL05',NULL,NULL,NULL,'Var', '083', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRL06',NULL,NULL,NULL,'Vaucluse', '084', 1.9);
INSERT INTO noisemodelling.nuts VALUES ('FRM',NULL,'Corse',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRM0',NULL,NULL,'Corse',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRM01',NULL,NULL,NULL,'Corse-du-Sud', '02A', 0.9);
INSERT INTO noisemodelling.nuts VALUES ('FRM02',NULL,NULL,NULL,'Haute-Corse', '02B', 0.9);
INSERT INTO noisemodelling.nuts VALUES ('FRY',NULL,'RUP FR - Régions Ultrapériphériques Françaises',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY1',NULL,NULL,'Guadeloupe',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY10',NULL,NULL,NULL,'Guadeloupe', '971', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRY2',NULL,NULL,'Martinique',NULL,NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY20',NULL,NULL,NULL,'Martinique', '972', 1.8);
INSERT INTO noisemodelling.nuts VALUES ('FRY3',NULL,NULL,'Guyane',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY30',NULL,NULL,NULL,'Guyane', '973', 2.7);
INSERT INTO noisemodelling.nuts VALUES ('FRY4',NULL,NULL,'La Réunion',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY40',NULL,NULL,NULL,'La Réunion', '974', 2.4);
INSERT INTO noisemodelling.nuts VALUES ('FRY5',NULL,NULL,'Mayotte',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRY50',NULL,NULL,NULL,'Mayotte', '976', 3.2);
INSERT INTO noisemodelling.nuts VALUES ('FRZ',NULL,'Extra-Regio nuts 1',NULL,NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRZZ',NULL,NULL,'Extra-Regio nuts 2',NULL,NULL, NULL);
INSERT INTO noisemodelling.nuts VALUES ('FRZZZ',NULL,NULL,NULL,'Extra-Regio nuts 3',NULL, NULL);


---------------------------------------------------------------------------------
-- 4- Update input tables with Cerema needs
---------------------------------------------------------------------------------

-- Remove buildings that are less than 20 square meters

DELETE FROM noisemodelling."C_BATIMENT_S_2154" WHERE ST_AREA(the_geom) < 20;
DELETE FROM noisemodelling."C_BATIMENT_S_2972" WHERE ST_AREA(the_geom) < 20;
DELETE FROM noisemodelling."C_BATIMENT_S_2975" WHERE ST_AREA(the_geom) < 20;
DELETE FROM noisemodelling."C_BATIMENT_S_4471" WHERE ST_AREA(the_geom) < 20;
DELETE FROM noisemodelling."C_BATIMENT_S_5490" WHERE ST_AREA(the_geom) < 20;


-- Force the building height to be equal to 7m when equal to 0
UPDATE noisemodelling."C_BATIMENT_S_2154" SET "BAT_HAUT"=7 WHERE "BAT_HAUT"=0;
UPDATE noisemodelling."C_BATIMENT_S_2972" SET "BAT_HAUT"=7 WHERE "BAT_HAUT"=0;
UPDATE noisemodelling."C_BATIMENT_S_2975" SET "BAT_HAUT"=7 WHERE "BAT_HAUT"=0;
UPDATE noisemodelling."C_BATIMENT_S_4471" SET "BAT_HAUT"=7 WHERE "BAT_HAUT"=0;
UPDATE noisemodelling."C_BATIMENT_S_5490" SET "BAT_HAUT"=7 WHERE "BAT_HAUT"=0;


---------------------------------------------------------------------------------
-- 5- Generate INFRA table, used in order to filter DEM tables
---------------------------------------------------------------------------------

-- For 2154
DROP TABLE IF EXISTS noisemodelling.infra_road_2154;
CREATE TABLE noisemodelling.infra_road_2154 AS SELECT a.the_geom
	FROM 
     	noisemodelling."N_ROUTIER_TRONCON_L_2154" a,
     	echeance4."N_ROUTIER_ROUTE" b 
    WHERE 
	    ST_LENGTH(a.the_geom)>0 and
	    a."CBS_GITT" and
	    b."CONCESSION"='N' and 
	    a."IDROUTE"=b."IDROUTE";

DROP TABLE IF EXISTS noisemodelling.infra_rail_2154;
CREATE TABLE noisemodelling.infra_rail_2154 AS SELECT a.the_geom
	FROM 
        noisemodelling."N_FERROVIAIRE_TRONCON_L_2154" a
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"='412280737';

DROP TABLE IF EXISTS noisemodelling.infra_2154;
CREATE TABLE noisemodelling.infra_2154 AS 
	SELECT the_geom FROM noisemodelling.infra_road_2154 UNION ALL
	SELECT the_geom FROM noisemodelling.infra_rail_2154;
CREATE INDEX infra_2154_geom_idx ON noisemodelling.infra_2154 USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling.infra_road_2154, noisemodelling.infra_rail_2154;


-- For 2972
DROP TABLE IF EXISTS noisemodelling.infra_road_2972;
CREATE TABLE noisemodelling.infra_road_2972 AS SELECT a.the_geom
	FROM 
     	noisemodelling."N_ROUTIER_TRONCON_L_2972" a,
     	echeance4."N_ROUTIER_ROUTE" b 
    WHERE 
	    ST_LENGTH(a.the_geom)>0 and
	    a."CBS_GITT" and
	    b."CONCESSION"='N' and 
	    a."IDROUTE"=b."IDROUTE";

DROP TABLE IF EXISTS noisemodelling.infra_rail_2972;
CREATE TABLE noisemodelling.infra_rail_2972 AS SELECT a.the_geom
	FROM 
        noisemodelling."N_FERROVIAIRE_TRONCON_L_2972" a
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"='412280737';

DROP TABLE IF EXISTS noisemodelling.infra_2972;
CREATE TABLE noisemodelling.infra_2972 AS 
	SELECT the_geom FROM noisemodelling.infra_road_2972 UNION ALL
	SELECT the_geom FROM noisemodelling.infra_rail_2972;
CREATE INDEX infra_2972_geom_idx ON noisemodelling.infra_2972 USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling.infra_road_2972, noisemodelling.infra_rail_2972;

-- For 2975
DROP TABLE IF EXISTS noisemodelling.infra_road_2975;
CREATE TABLE noisemodelling.infra_road_2975 AS SELECT a.the_geom
	FROM 
     	noisemodelling."N_ROUTIER_TRONCON_L_2975" a,
     	echeance4."N_ROUTIER_ROUTE" b 
    WHERE 
	    ST_LENGTH(a.the_geom)>0 and
	    a."CBS_GITT" and
	    b."CONCESSION"='N' and 
	    a."IDROUTE"=b."IDROUTE";

DROP TABLE IF EXISTS noisemodelling.infra_rail_2975;
CREATE TABLE noisemodelling.infra_rail_2975 AS SELECT a.the_geom
	FROM 
        noisemodelling."N_FERROVIAIRE_TRONCON_L_2975" a
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"='412280737';

DROP TABLE IF EXISTS noisemodelling.infra_2975;
CREATE TABLE noisemodelling.infra_2975 AS 
	SELECT the_geom FROM noisemodelling.infra_road_2975 UNION ALL
	SELECT the_geom FROM noisemodelling.infra_rail_2975;
CREATE INDEX infra_2975_geom_idx ON noisemodelling.infra_2975 USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling.infra_road_2975, noisemodelling.infra_rail_2975;

-- For 4471
DROP TABLE IF EXISTS noisemodelling.infra_road_4471;
CREATE TABLE noisemodelling.infra_road_4471 AS SELECT a.the_geom
	FROM 
     	noisemodelling."N_ROUTIER_TRONCON_L_4471" a,
     	echeance4."N_ROUTIER_ROUTE" b 
    WHERE 
	    ST_LENGTH(a.the_geom)>0 and
	    a."CBS_GITT" and
	    b."CONCESSION"='N' and 
	    a."IDROUTE"=b."IDROUTE";

DROP TABLE IF EXISTS noisemodelling.infra_rail_4471;
CREATE TABLE noisemodelling.infra_rail_4471 AS SELECT a.the_geom
	FROM 
        noisemodelling."N_FERROVIAIRE_TRONCON_L_4471" a
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"='412280737';

DROP TABLE IF EXISTS noisemodelling.infra_4471;
CREATE TABLE noisemodelling.infra_4471 AS 
	SELECT the_geom FROM noisemodelling.infra_road_4471 UNION ALL
	SELECT the_geom FROM noisemodelling.infra_rail_4471;
CREATE INDEX infra_4471_geom_idx ON noisemodelling.infra_4471 USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling.infra_road_4471, noisemodelling.infra_rail_4471;

-- For 5490
DROP TABLE IF EXISTS noisemodelling.infra_road_5490;
CREATE TABLE noisemodelling.infra_road_5490 AS SELECT a.the_geom
	FROM 
     	noisemodelling."N_ROUTIER_TRONCON_L_5490" a,
     	echeance4."N_ROUTIER_ROUTE" b 
    WHERE 
	    ST_LENGTH(a.the_geom)>0 and
	    a."CBS_GITT" and
	    b."CONCESSION"='N' and 
	    a."IDROUTE"=b."IDROUTE";

DROP TABLE IF EXISTS noisemodelling.infra_rail_5490;
CREATE TABLE noisemodelling.infra_rail_5490 AS SELECT a.the_geom
	FROM 
        noisemodelling."N_FERROVIAIRE_TRONCON_L_5490" a
    WHERE
        ST_LENGTH(a.the_geom)>0 and 
        a."CBS_GITT" and
        a."REFPROD"='412280737';

DROP TABLE IF EXISTS noisemodelling.infra_5490;
CREATE TABLE noisemodelling.infra_5490 AS 
	SELECT the_geom FROM noisemodelling.infra_road_5490 UNION ALL
	SELECT the_geom FROM noisemodelling.infra_rail_5490;
CREATE INDEX infra_5490_geom_idx ON noisemodelling.infra_5490 USING gist (the_geom);

DROP TABLE IF EXISTS noisemodelling.infra_road_5490, noisemodelling.infra_rail_5490;


---------------------------------------------------------------------------------
-- 6- Identify buildings that are in agglomeration
---------------------------------------------------------------------------------

-- NB : only the metropole (2154) has agglomeration

-- Add a boolean new column called AGGLO (default value is false)
ALTER TABLE noisemodelling."C_BATIMENT_S_2154" ADD COLUMN "AGGLO" boolean DEFAULT false;
ALTER TABLE noisemodelling."C_BATIMENT_S_2972" ADD COLUMN "AGGLO" boolean DEFAULT false;
ALTER TABLE noisemodelling."C_BATIMENT_S_2975" ADD COLUMN "AGGLO" boolean DEFAULT false;
ALTER TABLE noisemodelling."C_BATIMENT_S_4471" ADD COLUMN "AGGLO" boolean DEFAULT false;
ALTER TABLE noisemodelling."C_BATIMENT_S_5490" ADD COLUMN "AGGLO" boolean DEFAULT false;


-- Generate agglomerations for metropole (2154)
DROP TABLE IF EXISTS noisemodelling.agglo_2154;
CREATE TABLE noisemodelling.agglo_2154 AS SELECT 
	ST_TRANSFORM(ST_SetSRID(the_geom,4326), 2154) as the_geom, "AUTCOMCBS" as autcomcbs, "CODEDEPT"  as codedept, "UUEID" as uueid 
	FROM echeance4."C_CONTOUR_AUTCOMCBS_S";

CREATE INDEX agglo_2154_geom_idx ON noisemodelling.agglo_2154 USING gist (the_geom);
CREATE INDEX agglo_2154_insee_dep_idx ON noisemodelling.agglo_2154 USING btree (uueid);

-- Merge geometries (useful for the Paris region where several agglomerations touch each other)
DROP TABLE IF EXISTS noisemodelling.agglo_2154_unify;
CREATE TABLE noisemodelling.agglo_2154_unify AS SELECT (ST_Dump(ST_Union(ST_Accum(the_geom)))).geom as the_geom FROM noisemodelling.agglo_2154;
CREATE INDEX agglo_2154_unify_geom_idx ON noisemodelling.agglo_2154_unify USING gist (the_geom);

-- Selection of the identifier of buildings that intersect an agglomeration
DROP TABLE IF EXISTS noisemodelling.building_2154_agglo;
CREATE TABLE noisemodelling.building_2154_agglo AS SELECT a."IDBAT" FROM noisemodelling."C_BATIMENT_S_2154" a, noisemodelling.agglo_2154_unify b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
CREATE INDEX building_2154_agglo_idbat_idx ON noisemodelling.building_2154_agglo USING btree ("IDBAT"); 

-- Update AGGLO column (= true) for buildings which ID is in 'building_2154_agglo" table
UPDATE noisemodelling."C_BATIMENT_S_2154" SET "AGGLO" = true WHERE "IDBAT" in (SELECT "IDBAT" FROM noisemodelling.building_2154_agglo);

DROP TABLE IF EXISTS noisemodelling.building_2154_agglo;
