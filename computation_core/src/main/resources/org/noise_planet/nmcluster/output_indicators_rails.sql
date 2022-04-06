-- Calcul ERPS nico
SET @UUEID={$UUEID};

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
