DROP SCHEMA IF EXISTS cbs_uge_input CASCADE;

DROP ROLE IF EXISTS cbs_uge_group;
CREATE ROLE cbs_uge_group;

-- Grant ability to read all data
GRANT pg_read_all_data TO cbs_uge_group;

-- Grant ability to write all data
GRANT pg_write_all_data TO cbs_uge_group;

CREATE SCHEMA cbs_uge_input;


CREATE TABLE cbs_uge_input.nm_conf
(
    confid                     integer NOT NULL,
    confreflorder              integer,
    confmaxsrcdist             integer,
    confmaxrefldist            integer,
    confdistbuildingsreceivers integer,
    confthreadnumber           integer,
    confdiffvertical           boolean,
    confdiffhorizontal         boolean,
    confskiplday               boolean,
    confskiplevening           boolean,
    confskiplnight             boolean,
    confskiplden               boolean,
    confexportsourceid         boolean,
    wall_alpha                 real
);

CREATE TABLE cbs_uge_input.c_batiment_s_hexa
(
    geom3d public.geometry(MultiPolygonZ,2154),
    bat_idtopo character varying(24),
    bat_nature character varying,
    bat_nb_niv integer,
    pkey       integer,
    annee      character varying(4),
    refprod    character varying(9),
    origin_bat character varying(32),
    bat_pnb    character varying(32),
    bat_ppbe   character varying(32),
    bat_uueid  character varying(32),
    idbat      character varying(32),
    bat_haut   numeric(7, 1)
);


CREATE TABLE cbs_uge_input.c_population_hexa
(
    idpop    character varying(32),
    annee    character varying(4),
    codedept character varying(3),
    refprod  character varying(9),
    idbat    character varying(32),
    pop_orig character varying(32),
    pop_bat  numeric
);

CREATE TABLE cbs_uge_input.c_batimentsensible_hexa
(
    iderps      character varying(32),
    annee       character varying(4),
    codedept    character varying(2),
    refprod     character varying(9),
    erps_nature character varying(32),
    erps_ssnat  character varying(254),
    erps_orig   character varying(254),
    erps_nom    character varying(254),
    erps_capac  integer,
    erps_ouvrt  character varying(4),
    erps_idpai  character varying(24),
    erps_iduai  character varying(24),
    erps_idfin  character varying(24),
    erps_idsur  character varying(48),
    erps_idspo  character varying(24)
);



CREATE TABLE cbs_uge_input.n_routier_protection_acoustique_hexa
(
    idprotacou character varying(32),
    annee      character varying(4),
    codedept   character varying(3),
    refprod    character varying(9),
    idroute    character varying(32),
    nomroute   character varying(6),
    inseecomd  character varying(5),
    inseecomr  character varying(5),
    refsource  character varying(16),
    millsource character varying(4),
    idsource   character varying(33),
    typeprot   character varying(2),
    nomprot    character varying(32),
    prdeb      character varying(8),
    prfin      character varying(8),
    longueur   bigint,
    zdeb       double precision,
    zfin       double precision,
    hauteur    double precision,
    propriete  character varying(2),
    materiau1  character varying(2),
    materiau2  character varying(2),
    accessoire character varying(2),
    vegetalise character varying(1),
    inclinaiso bigint,
    support    character varying(2),
    validedeb  date,
    validefin  date,
    id         bigint,
    geom public.geometry
);

CREATE TABLE cbs_uge_input.n_routier_troncon_l_hexa
(
    idtroncon  character varying,
    annee      character varying(4),
    codedept   character varying,
    refprod    character varying(9),
    homogene   character varying(2),
    refgest    character varying(9),
    idroute    character varying(50),
    nomrueg    character varying(70),
    nomrued    character varying(70),
    inseecomg  character varying(5),
    inseecomd  character varying(5),
    refsource  character varying(16),
    millsource date,
    idsource   character varying(32),
    prdeb      character varying(8),
    prfin      character varying(8),
    zdeb       numeric,
    zfin       numeric,
    sens       character varying(2),
    largeur    numeric,
    nb_voies   numeric,
    repartitio character varying(32),
    franchisst character varying(13),
    validedeb  character varying(29),
    validefin  character varying(29),
    cbs_gitt   boolean,
    uueid      character varying(20),
    type_jonct numeric,
    dist_jonct numeric,
    agglo      character varying(254),
    geom public.geometry,
    pos_sol    character varying(20)
);


CREATE TABLE cbs_uge_input.nm_stations_hexa
(
    id_station     integer,
    the_geom public.geometry(Point,2154),
    type_station   character varying,
    lat            double precision,
    lon            double precision,
    temp_6_18      double precision,
    hygro_6_18     double precision,
    temp_18_22     double precision,
    hygro_18_22    double precision,
    temp_22_6      double precision,
    hygro_22_6     double precision,
    pfav_6_18      text,
    pfav_18_22     text,
    pfav_22_6      text,
    pfav_6_22      text,
    pfav_6_18_0    double precision,
    pfav_18_22_0   double precision,
    pfav_22_6_0    double precision,
    pfav_6_22_0    double precision,
    pfav_6_18_20   double precision,
    pfav_18_22_20  double precision,
    pfav_22_6_20   double precision,
    pfav_6_22_20   double precision,
    pfav_6_18_40   double precision,
    pfav_18_22_40  double precision,
    pfav_22_6_40   double precision,
    pfav_6_22_40   double precision,
    pfav_6_18_60   double precision,
    pfav_18_22_60  double precision,
    pfav_22_6_60   double precision,
    pfav_6_22_60   double precision,
    pfav_6_18_80   double precision,
    pfav_18_22_80  double precision,
    pfav_22_6_80   double precision,
    pfav_6_22_80   double precision,
    pfav_6_18_100  double precision,
    pfav_18_22_100 double precision,
    pfav_22_6_100  double precision,
    pfav_6_22_100  double precision,
    pfav_6_18_120  double precision,
    pfav_18_22_120 double precision,
    pfav_22_6_120  double precision,
    pfav_6_22_120  double precision,
    pfav_6_18_140  double precision,
    pfav_18_22_140 double precision,
    pfav_22_6_140  double precision,
    pfav_6_22_140  double precision,
    pfav_6_18_160  double precision,
    pfav_18_22_160 double precision,
    pfav_22_6_160  double precision,
    pfav_6_22_160  double precision,
    pfav_6_18_180  double precision,
    pfav_18_22_180 double precision,
    pfav_22_6_180  double precision,
    pfav_6_22_180  double precision,
    pfav_6_18_200  double precision,
    pfav_18_22_200 double precision,
    pfav_22_6_200  double precision,
    pfav_6_22_200  double precision,
    pfav_6_18_220  double precision,
    pfav_18_22_220 double precision,
    pfav_22_6_220  double precision,
    pfav_6_22_220  double precision,
    pfav_6_18_240  double precision,
    pfav_18_22_240 double precision,
    pfav_22_6_240  double precision,
    pfav_6_22_240  double precision,
    pfav_6_18_260  double precision,
    pfav_18_22_260 double precision,
    pfav_22_6_260  double precision,
    pfav_6_22_260  double precision,
    pfav_6_18_280  double precision,
    pfav_18_22_280 double precision,
    pfav_22_6_280  double precision,
    pfav_6_22_280  double precision,
    pfav_6_18_300  double precision,
    pfav_18_22_300 double precision,
    pfav_22_6_300  double precision,
    pfav_6_22_300  double precision,
    pfav_6_18_320  double precision,
    pfav_18_22_320 double precision,
    pfav_22_6_320  double precision,
    pfav_6_22_320  double precision,
    pfav_6_18_340  double precision,
    pfav_18_22_340 double precision,
    pfav_22_6_340  double precision,
    pfav_6_22_340  double precision
);
CREATE TABLE cbs_uge_input.n_routier_trafic_hexa
(
    idtrafic   character varying,
    annee      character varying(4),
    codedept   character varying,
    refprod    character varying(20),
    idtroncon  character varying,
    comment    character varying,
    prdeb      character varying,
    prfin      character varying,
    nomsource  character varying(50),
    sourcevl   character varying,
    sourcepl   character varying,
    source2r   character varying,
    pcentpl    numeric,
    pcentmpl   double precision,
    pcenthpl   double precision,
    pcent2r    double precision,
    pcent2r4a  double precision,
    pcent2r4b  double precision,
    tmja_tronc numeric,
    tmja_cbs   numeric,
    tmjavlt    integer,
    tmhvld     integer,
    tmhvls     integer,
    tmhvln     integer,
    tmjaplt    integer,
    tmhpld     integer,
    tmhpls     integer,
    tmhpln     integer,
    tmja2rt    integer,
    tmh2rd     integer,
    tmh2rs     integer,
    tmh2rn     integer,
    milltrafic date,
    debitsatac integer,
    saturation boolean
);


CREATE TABLE cbs_uge_input.n_routier_vitesse_hexa
(
    idvitesse  character varying(32),
    annee      character varying(4),
    codedept   character varying,
    refprod    character varying(9),
    idtroncon  character varying,
    comment    character varying(254),
    prdeb      character varying(8),
    prfin      character varying(8),
    typeacte   character varying(2),
    dateacte   character varying(29),
    natureacte character varying(2),
    sourcevit  character varying(2),
    vitessevl  numeric,
    vitessepl  numeric,
    vitesse4a  numeric(11, 0),
    vitesse4b  numeric(11, 0)
);

DROP SCHEMA IF EXISTS cbs_uge_output CASCADE;
CREATE SCHEMA cbs_uge_output;




