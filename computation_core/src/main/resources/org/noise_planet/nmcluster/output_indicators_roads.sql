-- Calcul ERPS nico
SET @UUEID='${UUEID}';

-----------------------------------------------------------------------------
-- Receiver Exposition for LDEN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, (CASE WHEN LAEQ < 55 THEN NULL WHEN LAEQ < 60 THEN 'Lden5559' WHEN LAEQ < 65 THEN 'Lden6064' WHEN LAEQ < 70 THEN 'Lden6569' WHEN LAEQ < 75 THEN 'Lden7074' ELSE 'LdenGreaterThan75' END) NOISELEVEL, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LDEN_ROADS L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
create index receiver_expo_BUILD_PK on receiver_expo(BUILD_PK);
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- remove out of bounds LAEQ
DELETE FROM receiver_expo WHERE NOISELEVEL IS NULL;
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_noiselevel on receiver_expo(noiselevel);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET EXPOSEDPEOPLE = COALESCE((SELECT ROUND(SUM(POP)) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')), EXPOSEDPEOPLE),  exposedDwellings = COALESCE(ROUND((SELECT SUM(POP) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')) / (SELECT RATIO_POP_LOG FROM METADATA)), exposedDwellings) WHERE UUEID = @UUEID;

-----------------------------------------------------------------------------
-- Population Receiver Exposition for LN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, (CASE WHEN LAEQ < 50 THEN NULL WHEN LAEQ < '55' THEN 'Lnight5054' WHEN LAEQ < 60 THEN 'Lnight5559' WHEN LAEQ < 65 THEN 'Lnight6064' WHEN LAEQ < 70 THEN 'Lnight6569' ELSE 'LnightGreaterThan70' END) NOISELEVEL, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LNIGHT_ROADS L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
create index receiver_expo_BUILD_PK on receiver_expo(BUILD_PK);
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- remove out of bounds LAEQ
DELETE FROM receiver_expo WHERE NOISELEVEL IS NULL;
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_noiselevel on receiver_expo(noiselevel);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET EXPOSEDPEOPLE = COALESCE((SELECT ROUND(SUM(POP)) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')), EXPOSEDPEOPLE),  exposedDwellings = COALESCE(ROUND((SELECT SUM(POP) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')) / (SELECT RATIO_POP_LOG FROM METADATA)), exposedDwellings) WHERE UUEID = @UUEID;

-----------------------------------------------------------------------------
-- Counting of EXPOSED structures
DROP TABLE IF EXISTS BUILDINGS_MAX_ERPS;
-- Add new column for conversion LAEQ double to NOISELEVEL varchar
CREATE TABLE BUILDINGS_MAX_ERPS(ID_ERPS VARCHAR,ERPS_NATUR VARCHAR, AGGLO BOOLEAN, PERIOD VARCHAR(4), LAEQ DECIMAL(5,2));

INSERT INTO BUILDINGS_MAX_ERPS SELECT id_erps, ERPS_NATUR, B.AGGLO, 'LDEN' PERIOD, max(LAEQ) - 3 as LAEQ FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LDEN_ROADS LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;

INSERT INTO BUILDINGS_MAX_ERPS SELECT id_erps, ERPS_NATUR, B.AGGLO, 'LN' PERIOD, max(LAEQ) - 3 as LAEQ FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LNIGHT_ROADS LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;
-- Add new column for conversion LAEQ double to NOISELEVEL varchar
ALTER TABLE BUILDINGS_MAX_ERPS ADD COLUMN NOISELEVEL VARCHAR USING (CASE WHEN PERIOD = 'LDEN' THEN
 (CASE WHEN LAEQ < 55 THEN NULL WHEN LAEQ < 60 THEN 'Lden5559' WHEN LAEQ < 65 THEN 'Lden6064' WHEN
 LAEQ < 70 THEN 'Lden6569' WHEN LAEQ < 75 THEN 'Lden7074' ELSE 'LdenGreaterThan75' END) ELSE
  (CASE WHEN LAEQ < 50 THEN NULL WHEN LAEQ < '55' THEN 'Lnight5054' WHEN LAEQ < 60 THEN 'Lnight5559' WHEN
  LAEQ < 65 THEN 'Lnight6064' WHEN LAEQ < 70 THEN 'Lnight6569' ELSE 'LnightGreaterThan70' END) END);

DELETE FROM BUILDINGS_MAX_ERPS WHERE NOISELEVEL IS NULL;