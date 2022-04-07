-- Calcul ERPS nico
SET @UUEID='${UUEID}';

-----------------------------------------------------------------------------
-- Receiver Exposition for LDEN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, (CASE WHEN LAEQ < 55 THEN NULL WHEN LAEQ < 60 THEN 'Lden5559' WHEN LAEQ < 65 THEN 'Lden6064' WHEN LAEQ < 70 THEN 'Lden6569' WHEN LAEQ < 75 THEN 'Lden7074' ELSE 'LdenGreaterThan75' END) NOISELEVEL, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LDEN_RAILWAY L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- remove out of bounds LAEQ
DELETE FROM receiver_expo WHERE NOISELEVEL IS NULL;
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_noiselevel on receiver_expo(noiselevel);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET EXPOSEDPEOPLE = COALESCE((SELECT ROUND(SUM(POP)) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')), EXPOSEDPEOPLE) WHERE UUEID = @UUEID;

-----------------------------------------------------------------------------
-- Receiver Exposition for LN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, (CASE WHEN LAEQ < 50 THEN NULL WHEN LAEQ < '55' THEN 'Lnight5054' WHEN LAEQ < 60 THEN 'Lnight5559' WHEN LAEQ < 65 THEN 'Lnight6064' WHEN LAEQ < 70 THEN 'Lnight6569' ELSE 'LnightGreaterThan70' END) NOISELEVEL, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LNIGHT_RAILWAY L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- remove out of bounds LAEQ
DELETE FROM receiver_expo WHERE NOISELEVEL IS NULL;
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_noiselevel on receiver_expo(noiselevel);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET EXPOSEDPEOPLE = COALESCE((SELECT ROUND(SUM(POP)) popsum FROM receiver_expo r WHERE r.NOISELEVEL = POPULATION_EXPOSURE.NOISELEVEL AND r.AGGLO = (exposureType = 'mostExposedFacadeIncludingAgglomeration')), EXPOSEDPEOPLE) WHERE UUEID = @UUEID;



INSERT INTO BUILDINGS_MAX SELECT @UUEID UUEID, ID_BAT, AGGLO, 'LDEN' PERIOD,max(LAEQ) - 3 noiselevel FROM BUILDINGS_SCREENS B INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK) INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1) INNER JOIN LDEN_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY ID_BAT, AGGLO;
INSERT INTO BUILDINGS_MAX SELECT @UUEID UUEID, ID_BAT, AGGLO, 'LN' PERIOD,max(LAEQ) - 3 noiselevel FROM BUILDINGS_SCREENS B INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK) INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1) INNER JOIN LNIGHT_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY ID_BAT, AGGLO;

INSERT INTO BUILDINGS_MAX_ERPS SELECT @UUEID UUEID, id_erps, ERPS_NATUR, B.AGGLO, 'LDEN' PERIOD, max(LAEQ) - 3 as noiselevel FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LDEN_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;

INSERT INTO BUILDINGS_MAX_ERPS SELECT @UUEID UUEID, id_erps, ERPS_NATUR, B.AGGLO, 'LN' PERIOD, max(LAEQ) - 3 as noiselevel FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LNIGHT_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;
