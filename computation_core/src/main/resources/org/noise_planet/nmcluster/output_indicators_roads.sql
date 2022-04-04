DROP TABLE IF EXISTS ALL_UUEID;
create table ALL_UUEID(UUEID varchar) AS SELECT DISTINCT UUEID FROM ROADS UNION ALL SELECT DISTINCT UUEID FROM RAIL_SECTIONS;
DROP TABLE IF EXISTS EXPOSURE_AREA;
create table EXPOSURE_AREA (noiseLevel varchar, uueid varchar, areaSquareKilometer double);
drop table if exists noise_level_class;
create table noise_level_class(exposureNoiseLevel varchar, noiseLevel varchar);
insert into noise_level_class values ('Lden55', 'Lden5559'), ('Lden55', 'Lden6064'), ('Lden55', 'Lden6569'), ('Lden55', 'Lden7074'),  ('Lden55', 'LdenGreaterThan75'),
 ('Lden65', 'Lden6569'), ('Lden65', 'Lden7074'),  ('Lden65', 'LdenGreaterThan75') ,
('Lden75', 'LdenGreaterThan75'), ('Lden5559',  'Lden5559'), ('Lden6064',  'Lden6064'), ('Lden6569', 'Lden6569'), ('Lden7074', 'Lden7074'),  ('LdenGreaterThan75', 'LdenGreaterThan75'),
('Lnight5054','Lnight5054'), ('Lnight5559','Lnight5559'), ('Lnight6064','Lnight6064'), ('Lnight6569','Lnight6569'), ('LnightGreaterThan70','LnightGreaterThan70');
insert into EXPOSURE_AREA select distinct exposureNoiseLevel, UUEID, 0 FROM noise_level_class, ALL_UUEID;
UPDATE EXPOSURE_AREA SET areaSquareKilometer = COALESCE((SELECT SUM(ST_AREA(THE_GEOM) / 1e-6) FROM CBS_A_R_LD_FRL02 C, noise_level_class N WHERE EXPOSURE_AREA.NOISELEVEL = N.exposureNoiseLevel AND N.noiseLevel = C.NOISELEVEL AND C.UUEID = EXPOSURE_AREA.UUEID GROUP BY exposureNoiseLevel), areaSquareKilometer);
UPDATE EXPOSURE_AREA SET areaSquareKilometer = COALESCE((SELECT SUM(ST_AREA(THE_GEOM) / 1e-6) FROM CBS_A_R_LN_FRL02 C, noise_level_class N WHERE EXPOSURE_AREA.NOISELEVEL = N.exposureNoiseLevel AND N.noiseLevel = C.NOISELEVEL AND C.UUEID = EXPOSURE_AREA.UUEID GROUP BY exposureNoiseLevel), areaSquareKilometer);

-- SELECT PK_1, LAEQ, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC) RECEIVER_RANK  FROM LDEN_ROADS L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE L.IDRECEIVER = RU.PK AND PK_1 = RB.PK HAVING