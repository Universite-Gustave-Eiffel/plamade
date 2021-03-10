------------------------------------------------------------------------------------
-- Script de calcul de la population impactée par plus de 65 db, par route        --
-- Auteur : Gwendall Petit (Lab-STICC - CNRS UMR 6285)                            --
-- Dernière mise à jour : 03/2021                                                 --
------------------------------------------------------------------------------------


-- 1- Pour une route, on va chercher tous les tronçons et donc les LDEN associés. 
--    Au passage, les décibels (champs LAEQ) sont convertis en pascal : LAEQpa = power(10,LAEQ/10)
DROP TABLE IF EXISTS LDEN_GEOM_ROADS;
CREATE TABLE LDEN_GEOM_ROADS AS 
	SELECT a.the_geom, a.idreceiver, a.idsource, a.laeq, power(10,a.laeq/10) as laeqpa,
	b.id_troncon, b.id_route, b.nom_route, b.pk 
	FROM LDEN_GEOM a, roads b 
	WHERE a.idsource=b.pk;


-- 2- Pour chacun des récepteurs, on va faire la somme acoustique 
-- 	  Somme LAEQpa = 10*log10(sum(LAEQpa)) (1 valeur par récepteurs)

DROP TABLE IF EXISTS RECEIVERS_SUM_LAEQPA;
CREATE TABLE RECEIVERS_SUM_LAEQPA AS SELECT 
	st_union(st_accum(the_geom)) as the_geom, id_route, nom_route, idreceiver, 
	10*log10(sum(LAEQpa)) as laeqpa_sum 
	FROM LDEN_GEOM_ROADS
	GROUP BY idreceiver, id_route, nom_route 
	ORDER BY id_route, idreceiver;

-- 3- On ne garde que les récepteurs qui ont une somme acoustique supérieure à 65db

DROP TABLE IF EXISTS RECEIVERS_POP;
CREATE TABLE RECEIVERS_POP AS SELECT 
	a.ID_ROUTE, a.nom_route, a.idreceiver, b.pop as pop
	FROM RECEIVERS_SUM_LAEQPA a, receivers b 	
	WHERE a.idreceiver=b.PK and a.laeqpa_sum>65;


-- 4- Puis on somme la population en la regroupant par route

DROP TABLE IF EXISTS ROADS_POP;
CREATE TABLE ROADS_POP AS SELECT ID_ROUTE, nom_route, SUM(pop) as sum_pop
FROM RECEIVERS_POP
GROUP BY ID_ROUTE, nom_route;

