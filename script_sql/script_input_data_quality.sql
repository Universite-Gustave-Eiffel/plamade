------------------------------------------------------------------------------------
-- Script d'analyse de la qualité des données source issues de la base Plamade    --
-- Auteur : Gwendall Petit (Lab-STICC - CNRS UMR 6285)                            --
-- Dernière mise à jour : 04/2021                                                 --
------------------------------------------------------------------------------------


---------------------------------------------------------------------------------
-- 1- For road
---------------------------------------------------------------------------------

-- For road trafic

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_nb;
CREATE TABLE noisemodelling.stat_road_trafic_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_trafic FROM echeance4."N_ROUTIER_TRAFIC" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhvld_0, noisemodelling.stat_road_trafic_tmhvld_null;

CREATE TABLE noisemodelling.stat_road_trafic_tmhvld_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvld_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLD" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhvld_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvld_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLD" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhvls_0, noisemodelling.stat_road_trafic_tmhvls_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmhvls_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvls_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLS" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhvls_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvls_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLS" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhvln_0, noisemodelling.stat_road_trafic_tmhvln_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmhvln_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvln_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLN" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhvln_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhvln_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHVLN" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhpld_0, noisemodelling.stat_road_trafic_tmhpld_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmhpld_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpld_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLD" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhpld_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpld_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLD" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhpls_0, noisemodelling.stat_road_trafic_tmhpls_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmhpls_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpls_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLS" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhpls_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpls_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLS" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmhpln_0, noisemodelling.stat_road_trafic_tmhpln_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmhpln_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpln_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLN" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmhpln_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmhpln_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMHPLN" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmh2rd_0, noisemodelling.stat_road_trafic_tmh2rd_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rd_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rd_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RD" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rd_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rd_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RD" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmh2rs_0, noisemodelling.stat_road_trafic_tmh2rs_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rs_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rs_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RS" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rs_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rs_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RS" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_tmh2rn_0, noisemodelling.stat_road_trafic_tmh2rn_null;
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rn_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rn_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RN" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_tmh2rn_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as tmh2rn_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "TMH2RN" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_pcentmpl_0, noisemodelling.stat_road_trafic_pcentmpl_null;
CREATE TABLE noisemodelling.stat_road_trafic_pcentmpl_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcentmpl_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENTMPL" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_pcentmpl_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcentmpl_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENTMPL" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_pcenthpl_0, noisemodelling.stat_road_trafic_pcenthpl_null;
CREATE TABLE noisemodelling.stat_road_trafic_pcenthpl_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcenthpl_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENTHPL" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_pcenthpl_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcenthpl_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENTHPL" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_pcent2r4a_0, noisemodelling.stat_road_trafic_pcent2r4a_null;
CREATE TABLE noisemodelling.stat_road_trafic_pcent2r4a_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcent2r4a_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENT2R4A" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_pcent2r4a_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcent2r4a_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENT2R4A" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_pcent2r4b_0, noisemodelling.stat_road_trafic_pcent2r4b_null;
CREATE TABLE noisemodelling.stat_road_trafic_pcent2r4b_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcent2r4b_0 FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENT2R4B" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_trafic_pcent2r4b_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as pcent2r4b_null FROM echeance4."N_ROUTIER_TRAFIC" WHERE "PCENT2R4B" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";


DROP TABLE IF EXISTS noisemodelling.stat_road_trafic;
CREATE TABLE noisemodelling.stat_road_trafic AS SELECT a.*, 
		b.tmhvld_0, c.tmhvld_null, 
		d.tmhvls_0, e.tmhvls_null, 
		f.tmhvln_0, g.tmhvln_null, 
		h.tmhpld_0, i.tmhpld_null, 
		j.tmhpls_0, k.tmhpls_null, 
		l.tmhpln_0, m.tmhpln_null, 
		n.tmh2rd_0, o.tmh2rd_null, 
		p.tmh2rs_0, q.tmh2rs_null, 
		r.tmh2rn_0, s.tmh2rn_null, 
		t.pcentmpl_0, u.pcentmpl_null, 
		v.pcenthpl_0, w.pcenthpl_null, 
		x.pcent2r4a_0, y.pcent2r4a_null, 
		z.pcent2r4b_0, aa.pcent2r4b_null 
	FROM noisemodelling.stat_road_trafic_nb a 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvld_0 b ON a.codedept=b.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvld_null c ON a.codedept=c.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvls_0 d ON a.codedept=d.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvls_null e ON a.codedept=e.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvln_0 f ON a.codedept=f.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhvln_null g ON a.codedept=g.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpld_0 h ON a.codedept=h.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpld_null i ON a.codedept=i.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpls_0 j ON a.codedept=j.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpls_null k ON a.codedept=k.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpln_0 l ON a.codedept=l.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmhpln_null m ON a.codedept=m.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rd_0 n ON a.codedept=n.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rd_null o ON a.codedept=o.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rs_0 p ON a.codedept=p.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rs_null q ON a.codedept=q.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rn_0 r ON a.codedept=r.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_tmh2rn_null s ON a.codedept=s.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcentmpl_0 t ON a.codedept=t.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcentmpl_null u ON a.codedept=u.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcenthpl_0 v ON a.codedept=v.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcenthpl_null w ON a.codedept=w.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcent2r4a_0 x ON a.codedept=x.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcent2r4a_null y ON a.codedept=y.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcent2r4b_0 z ON a.codedept=z.codedept 
	LEFT JOIN noisemodelling.stat_road_trafic_pcent2r4b_null aa ON a.codedept=aa.codedept 
	ORDER BY a.codedept;

DROP TABLE IF EXISTS noisemodelling.stat_road_trafic_nb, 
	noisemodelling.stat_road_trafic_tmhvld_0, noisemodelling.stat_road_trafic_tmhvld_null, 
	noisemodelling.stat_road_trafic_tmhvls_0, noisemodelling.stat_road_trafic_tmhvls_null, 
	noisemodelling.stat_road_trafic_tmhvln_0, noisemodelling.stat_road_trafic_tmhvln_null, 
	noisemodelling.stat_road_trafic_tmhpld_0, noisemodelling.stat_road_trafic_tmhpld_null,
	noisemodelling.stat_road_trafic_tmhpls_0, noisemodelling.stat_road_trafic_tmhpls_null,
	noisemodelling.stat_road_trafic_tmhpln_0, noisemodelling.stat_road_trafic_tmhpln_null,
	noisemodelling.stat_road_trafic_tmh2rd_0, noisemodelling.stat_road_trafic_tmh2rd_null,
	noisemodelling.stat_road_trafic_tmh2rs_0, noisemodelling.stat_road_trafic_tmh2rs_null,
	noisemodelling.stat_road_trafic_tmh2rn_0, noisemodelling.stat_road_trafic_tmh2rn_null, 
	noisemodelling.stat_road_trafic_pcentmpl_0, noisemodelling.stat_road_trafic_pcentmpl_null,
	noisemodelling.stat_road_trafic_pcenthpl_0, noisemodelling.stat_road_trafic_pcenthpl_null,
	noisemodelling.stat_road_trafic_pcent2r4a_0, noisemodelling.stat_road_trafic_pcent2r4a_null,
	noisemodelling.stat_road_trafic_pcent2r4b_0, noisemodelling.stat_road_trafic_pcent2r4b_null;

UPDATE noisemodelling.stat_road_trafic SET tmhvld_0 = 0 WHERE tmhvld_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhvld_null = 0 WHERE tmhvld_null is null;
UPDATE noisemodelling.stat_road_trafic SET tmhvls_0 = 0 WHERE tmhvls_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhvls_null = 0 WHERE tmhvls_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhvln_0 = 0 WHERE tmhvln_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhvln_null = 0 WHERE tmhvln_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpld_0 = 0 WHERE tmhpld_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpld_null = 0 WHERE tmhpld_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpls_0 = 0 WHERE tmhpls_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpls_null = 0 WHERE tmhpls_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpln_0 = 0 WHERE tmhpln_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmhpln_null = 0 WHERE tmhpln_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rd_0 = 0 WHERE tmh2rd_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rd_null = 0 WHERE tmh2rd_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rs_0 = 0 WHERE tmh2rs_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rs_null = 0 WHERE tmh2rs_null is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rn_0 = 0 WHERE tmh2rn_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET tmh2rn_null = 0 WHERE tmh2rn_null is null; 
UPDATE noisemodelling.stat_road_trafic SET pcentmpl_0 = 0 WHERE pcentmpl_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET pcentmpl_null = 0 WHERE pcentmpl_null is null; 
UPDATE noisemodelling.stat_road_trafic SET pcenthpl_0 = 0 WHERE pcenthpl_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET pcenthpl_null = 0 WHERE pcenthpl_null is null; 
UPDATE noisemodelling.stat_road_trafic SET pcent2r4a_0 = 0 WHERE pcent2r4a_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET pcent2r4a_null = 0 WHERE pcent2r4a_null is null; 
UPDATE noisemodelling.stat_road_trafic SET pcent2r4b_0 = 0 WHERE pcent2r4b_0 is null; 
UPDATE noisemodelling.stat_road_trafic SET pcent2r4b_null = 0 WHERE pcent2r4b_null is null;


-- For road speed

DROP TABLE IF EXISTS noisemodelling.stat_road_speed_nb;
CREATE TABLE noisemodelling.stat_road_speed_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_speed FROM echeance4."N_ROUTIER_VITESSE" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_speed_vitessevl_0, noisemodelling.stat_road_speed_vitessevl_null;
CREATE TABLE noisemodelling.stat_road_speed_vitessevl_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as vitessevl_0 FROM echeance4."N_ROUTIER_VITESSE" WHERE "VITESSEVL" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_speed_vitessevl_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as vitessevl_null FROM echeance4."N_ROUTIER_VITESSE" WHERE "VITESSEVL" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_speed_vitessepl_0, noisemodelling.stat_road_speed_vitessepl_null;
CREATE TABLE noisemodelling.stat_road_speed_vitessepl_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as vitessepl_0 FROM echeance4."N_ROUTIER_VITESSE" WHERE "VITESSEPL" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_speed_vitessepl_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as vitessepl_null FROM echeance4."N_ROUTIER_VITESSE" WHERE "VITESSEPL" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_speed;
CREATE TABLE noisemodelling.stat_road_speed AS SELECT a.*, 
		b.vitessevl_0, c.vitessevl_null, 
		d.vitessepl_0, e.vitessepl_null
	FROM noisemodelling.stat_road_speed_nb a 
	LEFT JOIN noisemodelling.stat_road_speed_vitessevl_0 b ON a.codedept=b.codedept 
	LEFT JOIN noisemodelling.stat_road_speed_vitessevl_null c ON a.codedept=c.codedept 
	LEFT JOIN noisemodelling.stat_road_speed_vitessepl_0 d ON a.codedept=d.codedept 
	LEFT JOIN noisemodelling.stat_road_speed_vitessepl_null e ON a.codedept=e.codedept 
	ORDER BY a.codedept;

UPDATE noisemodelling.stat_road_speed SET vitessevl_0 = 0 WHERE vitessevl_0 is null; 
UPDATE noisemodelling.stat_road_speed SET vitessevl_null = 0 WHERE vitessevl_null is null; 
UPDATE noisemodelling.stat_road_speed SET vitessepl_0 = 0 WHERE vitessepl_0 is null; 
UPDATE noisemodelling.stat_road_speed SET vitessepl_null = 0 WHERE vitessepl_null is null;

DROP TABLE IF EXISTS noisemodelling.stat_road_speed_nb, noisemodelling.stat_road_speed_vitessevl_0, noisemodelling.stat_road_speed_vitessevl_null, noisemodelling.stat_road_speed_vitessepl_0, noisemodelling.stat_road_speed_vitessepl_null;


-- For road geometries

DROP TABLE IF EXISTS noisemodelling.stat_road_nb;
CREATE TABLE noisemodelling.stat_road_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_track FROM echeance4."N_ROUTIER_TRONCON_L" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_name;
CREATE TABLE noisemodelling.stat_road_name AS SELECT "CODEDEPT" as codedept, COUNT(DISTINCT "IDROUTE") as nb_road FROM echeance4."N_ROUTIER_TRONCON_L" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_width_0, noisemodelling.stat_road_width_null;
CREATE TABLE noisemodelling.stat_road_width_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as largeur_0 FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "LARGEUR" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_width_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as largeur_null FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "LARGEUR" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_nb_track_0, noisemodelling.stat_road_nb_track_null;
CREATE TABLE noisemodelling.stat_road_nb_track_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_voies_0 FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "NB_VOIES" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_road_nb_track_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_voies_null FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "NB_VOIES" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_way_null;
CREATE TABLE noisemodelling.stat_road_way_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as sens_null FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "SENS" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_cbs_gitt_o;
CREATE TABLE noisemodelling.stat_road_cbs_gitt_o AS SELECT "CODEDEPT" as codedept, COUNT(*) as cbs_gitt_o FROM echeance4."N_ROUTIER_TRONCON_L" WHERE "CBS_GITT" is true GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_road_geom;
CREATE TABLE noisemodelling.stat_road_geom AS SELECT a.*, 
		b.nb_road, c.cbs_gitt_o,
		d.largeur_0, e.largeur_null, 
		f.nb_voies_0, g.nb_voies_null,
		h.sens_null 
	FROM noisemodelling.stat_road_nb a 
	LEFT JOIN noisemodelling.stat_road_name b ON a.codedept=b.codedept
	LEFT JOIN noisemodelling.stat_road_cbs_gitt_o c ON a.codedept=c.codedept  
	LEFT JOIN noisemodelling.stat_road_width_0 d ON a.codedept=d.codedept 
	LEFT JOIN noisemodelling.stat_road_width_null e ON a.codedept=e.codedept 
	LEFT JOIN noisemodelling.stat_road_nb_track_0 f ON a.codedept=f.codedept 
	LEFT JOIN noisemodelling.stat_road_nb_track_null g ON a.codedept=g.codedept 
	LEFT JOIN noisemodelling.stat_road_way_null h ON a.codedept=h.codedept 
	ORDER BY a.codedept;

UPDATE noisemodelling.stat_road_geom SET cbs_gitt_o = 0 WHERE cbs_gitt_o is null; 
UPDATE noisemodelling.stat_road_geom SET largeur_0 = 0 WHERE largeur_0 is null; 
UPDATE noisemodelling.stat_road_geom SET largeur_null = 0 WHERE largeur_null is null; 
UPDATE noisemodelling.stat_road_geom SET nb_voies_0 = 0 WHERE nb_voies_0 is null; 
UPDATE noisemodelling.stat_road_geom SET nb_voies_null = 0 WHERE nb_voies_null is null;
UPDATE noisemodelling.stat_road_geom SET sens_null = 0 WHERE sens_null is null;

DROP TABLE IF EXISTS noisemodelling.stat_road_nb, noisemodelling.stat_road_name, noisemodelling.stat_road_width_0, noisemodelling.stat_road_width_null, 
	noisemodelling.stat_road_nb_track_0, noisemodelling.stat_road_nb_track_null, noisemodelling.stat_road_way_null, noisemodelling.stat_road_cbs_gitt_o;


-- Merge road tables

DROP TABLE IF EXISTS noisemodelling.stat_road;
CREATE TABLE noisemodelling.stat_road AS SELECT a.*, 
		b.nb_speed, b.vitessevl_0, b.vitessevl_null, b.vitessepl_0, b.vitessepl_null,
		c.nb_trafic, c.tmhvld_0, c.tmhvld_null, c.tmhvls_0, c.tmhvls_null, c.tmhvln_0, c.tmhvln_null, c.tmhpld_0, c.tmhpld_null, c.tmhpls_0, 
		c.tmhpls_null, c.tmhpln_0, c.tmhpln_null, c.tmh2rd_0, c.tmh2rd_null, c.tmh2rs_0, c.tmh2rs_null, c.tmh2rn_0, c.tmh2rn_null, 
		c.pcentmpl_0, c.pcentmpl_null, c.pcenthpl_0, c.pcenthpl_null, c.pcent2r4a_0, c.pcent2r4a_null, c.pcent2r4b_0, c.pcent2r4b_null
	FROM noisemodelling.stat_road_geom a 
	LEFT JOIN noisemodelling.stat_road_speed b ON a.codedept=b.codedept
	LEFT JOIN noisemodelling.stat_road_trafic c ON a.codedept=c.codedept  
	ORDER BY a.codedept;

DROP TABLE IF EXISTS noisemodelling.stat_road_geom, noisemodelling.stat_road_speed, noisemodelling.stat_road_trafic;


---------------------------------------------------------------------------------
-- 2- For buildings
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS noisemodelling.stat_building_nb;
CREATE TABLE noisemodelling.stat_building_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_building FROM echeance4."C_BATIMENT_S" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_building_height_0, noisemodelling.stat_building_height_null;
CREATE TABLE noisemodelling.stat_building_height_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as height_0 FROM echeance4."C_BATIMENT_S" WHERE "BAT_HAUT" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_building_height_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as height_null FROM echeance4."C_BATIMENT_S" WHERE "BAT_HAUT" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_building;
CREATE TABLE noisemodelling.stat_building AS SELECT a.*, 
		b.height_0, c.height_null
	FROM noisemodelling.stat_building_nb a 
	LEFT JOIN noisemodelling.stat_building_height_0 b ON a.codedept=b.codedept
	LEFT JOIN noisemodelling.stat_building_height_null c ON a.codedept=c.codedept  
	ORDER BY a.codedept;

UPDATE noisemodelling.stat_building SET height_0 = 0 WHERE height_0 is null; 
UPDATE noisemodelling.stat_building SET height_null = 0 WHERE height_null is null; 

DROP TABLE IF EXISTS noisemodelling.stat_building_nb, noisemodelling.stat_building_height_0, noisemodelling.stat_building_height_null;


---------------------------------------------------------------------------------
-- 3- For Landcover
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS noisemodelling.stat_landcover_nb;
CREATE TABLE noisemodelling.stat_landcover_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_landcover FROM echeance4."C_NATURESOL_S" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_natsol_0, noisemodelling.stat_natsol_null;
CREATE TABLE noisemodelling.stat_natsol_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as natsol_0 FROM echeance4."C_NATURESOL_S" WHERE "NATSOL_CNO" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_natsol_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as natsol_null FROM echeance4."C_NATURESOL_S" WHERE "NATSOL_CNO" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_landcover;
CREATE TABLE noisemodelling.stat_landcover AS SELECT a.*, 
		b.natsol_0, c.natsol_null
	FROM noisemodelling.stat_landcover_nb a 
	LEFT JOIN noisemodelling.stat_natsol_0 b ON a.codedept=b.codedept
	LEFT JOIN noisemodelling.stat_natsol_null c ON a.codedept=c.codedept  
	ORDER BY a.codedept;

UPDATE noisemodelling.stat_landcover SET natsol_0 = 0 WHERE natsol_0 is null; 
UPDATE noisemodelling.stat_landcover SET natsol_null = 0 WHERE natsol_null is null; 

DROP TABLE IF EXISTS noisemodelling.stat_landcover_nb, noisemodelling.stat_natsol_0, noisemodelling.stat_natsol_null;


---------------------------------------------------------------------------------
-- 4- For Rails
---------------------------------------------------------------------------------

-- For rail geometries

DROP TABLE IF EXISTS noisemodelling.stat_rail_nb;
CREATE TABLE noisemodelling.stat_rail_nb AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_track FROM echeance4."N_FERROVIAIRE_TRONCON_L" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_line_nb;
CREATE TABLE noisemodelling.stat_rail_line_nb AS SELECT "CODEDEPT" as codedept, COUNT(DISTINCT "IDLIGNE") as nb_line FROM echeance4."N_FERROVIAIRE_TRONCON_L" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_width_0, noisemodelling.stat_rail_width_null;
CREATE TABLE noisemodelling.stat_rail_width_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as largempris_0 FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "LARGEMPRIS" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_rail_width_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as largempris_null FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "LARGEMPRIS" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_nb_track_0, noisemodelling.stat_rail_nb_track_null;
CREATE TABLE noisemodelling.stat_rail_nb_track_0 AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_voies_0 FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "NB_VOIES" = 0 GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";
CREATE TABLE noisemodelling.stat_rail_nb_track_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as nb_voies_null FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "NB_VOIES" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_vmax_null;
CREATE TABLE noisemodelling.stat_rail_vmax_null AS SELECT "CODEDEPT" as codedept, COUNT(*) as vmax_null FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "VMAXINFRA" is null GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_cbs_gitt_o;
CREATE TABLE noisemodelling.stat_rail_cbs_gitt_o AS SELECT "CODEDEPT" as codedept, COUNT(*) as cbs_gitt_o FROM echeance4."N_FERROVIAIRE_TRONCON_L" WHERE "CBS_GITT" GROUP BY "CODEDEPT" ORDER BY "CODEDEPT";

DROP TABLE IF EXISTS noisemodelling.stat_rail_geom;
CREATE TABLE noisemodelling.stat_rail_geom AS SELECT a.*, 
		b.nb_line, c.cbs_gitt_o,
		d.largempris_0, e.largempris_null, 
		f.nb_voies_0, g.nb_voies_null,
		h.vmax_null 
	FROM noisemodelling.stat_rail_nb a 
	LEFT JOIN noisemodelling.stat_rail_line_nb b ON a.codedept=b.codedept
	LEFT JOIN noisemodelling.stat_rail_cbs_gitt_o c ON a.codedept=c.codedept  
	LEFT JOIN noisemodelling.stat_rail_width_0 d ON a.codedept=d.codedept 
	LEFT JOIN noisemodelling.stat_rail_width_null e ON a.codedept=e.codedept 
	LEFT JOIN noisemodelling.stat_rail_nb_track_0 f ON a.codedept=f.codedept 
	LEFT JOIN noisemodelling.stat_rail_nb_track_null g ON a.codedept=g.codedept 
	LEFT JOIN noisemodelling.stat_rail_vmax_null h ON a.codedept=h.codedept 
	ORDER BY a.codedept;

UPDATE noisemodelling.stat_rail_geom SET cbs_gitt_o = 0 WHERE cbs_gitt_o is null; 
UPDATE noisemodelling.stat_rail_geom SET largempris_0 = 0 WHERE largempris_0 is null; 
UPDATE noisemodelling.stat_rail_geom SET largempris_null = 0 WHERE largempris_null is null; 
UPDATE noisemodelling.stat_rail_geom SET nb_voies_0 = 0 WHERE nb_voies_0 is null; 
UPDATE noisemodelling.stat_rail_geom SET nb_voies_null = 0 WHERE nb_voies_null is null;
UPDATE noisemodelling.stat_rail_geom SET vmax_null = 0 WHERE vmax_null is null;

DROP TABLE IF EXISTS noisemodelling.stat_rail_nb, noisemodelling.stat_rail_line_nb, noisemodelling.stat_rail_width_0, noisemodelling.stat_rail_width_null, 
noisemodelling.stat_rail_nb_track_0, noisemodelling.stat_rail_nb_track_null, noisemodelling.stat_rail_vmax_null, noisemodelling.stat_rail_cbs_gitt_o;