DROP SCHEMA IF EXISTS cbs_uge_input CASCADE;
DROP SCHEMA IF EXISTS cbs_uge_output CASCADE;
DROP SCHEMA IF EXISTS bd_alti CASCADE;
DROP SCHEMA IF EXISTS bd_topo CASCADE;

CREATE SCHEMA cbs_uge_output;
CREATE SCHEMA cbs_uge_input;
CREATE SCHEMA bd_alti;
CREATE SCHEMA bd_topo;

DROP ROLE IF EXISTS cbs_uge_group;
CREATE ROLE cbs_uge_group;

-- Grant ability to read all data
GRANT pg_read_all_data TO cbs_uge_group;

-- Grant ability to write all data
GRANT pg_write_all_data TO cbs_uge_group;

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


CREATE TABLE cbs_uge_input.c_correspond_batiment_batimentsensible_hexa
(
    idcorresp character varying(32),
    annee     character varying(4),
    codedept  character varying(2),
    refprod   character varying(9),
    idbat     character varying(32),
    iderps    character varying(32),
    geom3d public.geometry(MultiPolygonZ,2154)
);


CREATE TABLE cbs_uge_input.c_naturesol_hexa (
	geom public.geometry(multipolygon, 2154) NULL,
	idnatsol varchar(32) NULL,
	annee varchar(4) NULL,
	codedept varchar(3) NULL,
	refprod varchar(9) NULL,
	natsol_clc int4 NULL,
	natsol_cno float8 NULL,
	natsol_lib varchar(254) NULL,
	pkey int4 NULL
);

CREATE TABLE cbs_uge_input.nm_link_dept_infra_road_hexa (
	uueid varchar(20) NULL,
	insee_dep varchar(3) NULL,
	bd_alti varchar(4) NULL
);
CREATE INDEX nm_link_dept_infra_road_hexa_insee_dep ON cbs_uge_input.nm_link_dept_infra_road_hexa USING btree (insee_dep);
CREATE INDEX nm_link_dept_infra_road_hexa_uueid ON cbs_uge_input.nm_link_dept_infra_road_hexa USING btree (uueid);

--
-- Name: bd_alti; Type: SCHEMA; Schema: -; Owner: cbs_uge_group
--

ALTER SCHEMA bd_alti OWNER TO cbs_uge_group;

--
-- Name: SCHEMA bd_alti; Type: COMMENT; Schema: -; Owner: cbs_uge_group
--

COMMENT ON SCHEMA bd_alti IS 'schema spécifique pour les tables bd_alti utilisées par l''UGE';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: d001; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d001 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d001 OWNER TO cbs_uge_group;

--
-- Name: d002; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d002 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d002 OWNER TO cbs_uge_group;

--
-- Name: d003; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d003 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d003 OWNER TO cbs_uge_group;

--
-- Name: d004; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d004 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d004 OWNER TO cbs_uge_group;

--
-- Name: d005; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d005 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d005 OWNER TO cbs_uge_group;

--
-- Name: d006; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d006 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d006 OWNER TO cbs_uge_group;

--
-- Name: d007; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d007 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d007 OWNER TO cbs_uge_group;

--
-- Name: d008; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d008 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d008 OWNER TO cbs_uge_group;

--
-- Name: d009; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d009 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d009 OWNER TO cbs_uge_group;

--
-- Name: d010; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d010 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d010 OWNER TO cbs_uge_group;

--
-- Name: d011; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d011 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d011 OWNER TO cbs_uge_group;

--
-- Name: d012; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d012 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d012 OWNER TO cbs_uge_group;

--
-- Name: d013; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d013 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d013 OWNER TO cbs_uge_group;

--
-- Name: d014; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d014 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d014 OWNER TO cbs_uge_group;

--
-- Name: d015; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d015 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d015 OWNER TO cbs_uge_group;

--
-- Name: d016; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d016 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d016 OWNER TO cbs_uge_group;

--
-- Name: d017; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d017 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d017 OWNER TO cbs_uge_group;

--
-- Name: d018; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d018 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d018 OWNER TO cbs_uge_group;

--
-- Name: d019; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d019 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d019 OWNER TO cbs_uge_group;

--
-- Name: d021; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d021 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d021 OWNER TO cbs_uge_group;

--
-- Name: d022; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d022 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d022 OWNER TO cbs_uge_group;

--
-- Name: d023; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d023 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d023 OWNER TO cbs_uge_group;

--
-- Name: d024; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d024 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d024 OWNER TO cbs_uge_group;

--
-- Name: d025; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d025 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d025 OWNER TO cbs_uge_group;

--
-- Name: d026; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d026 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d026 OWNER TO cbs_uge_group;

--
-- Name: d027; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d027 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d027 OWNER TO cbs_uge_group;

--
-- Name: d028; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d028 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d028 OWNER TO cbs_uge_group;

--
-- Name: d029; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d029 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d029 OWNER TO cbs_uge_group;

--
-- Name: d02a; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d02a (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d02a OWNER TO cbs_uge_group;

--
-- Name: d02b; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d02b (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d02b OWNER TO cbs_uge_group;

--
-- Name: d030; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d030 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d030 OWNER TO cbs_uge_group;

--
-- Name: d031; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d031 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d031 OWNER TO cbs_uge_group;

--
-- Name: d032; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d032 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d032 OWNER TO cbs_uge_group;

--
-- Name: d033; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d033 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d033 OWNER TO cbs_uge_group;

--
-- Name: d034; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d034 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d034 OWNER TO cbs_uge_group;

--
-- Name: d035; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d035 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d035 OWNER TO cbs_uge_group;

--
-- Name: d036; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d036 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d036 OWNER TO cbs_uge_group;

--
-- Name: d037; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d037 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d037 OWNER TO cbs_uge_group;

--
-- Name: d038; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d038 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d038 OWNER TO cbs_uge_group;

--
-- Name: d039; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d039 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d039 OWNER TO cbs_uge_group;

--
-- Name: d040; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d040 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d040 OWNER TO cbs_uge_group;

--
-- Name: d041; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d041 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d041 OWNER TO cbs_uge_group;

--
-- Name: d042; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d042 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d042 OWNER TO cbs_uge_group;

--
-- Name: d043; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d043 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d043 OWNER TO cbs_uge_group;

--
-- Name: d044; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d044 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d044 OWNER TO cbs_uge_group;

--
-- Name: d045; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d045 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d045 OWNER TO cbs_uge_group;

--
-- Name: d046; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d046 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d046 OWNER TO cbs_uge_group;

--
-- Name: d047; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d047 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d047 OWNER TO cbs_uge_group;

--
-- Name: d048; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d048 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d048 OWNER TO cbs_uge_group;

--
-- Name: d049; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d049 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d049 OWNER TO cbs_uge_group;

--
-- Name: d050; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d050 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d050 OWNER TO cbs_uge_group;

--
-- Name: d051; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d051 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d051 OWNER TO cbs_uge_group;

--
-- Name: d052; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d052 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d052 OWNER TO cbs_uge_group;

--
-- Name: d053; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d053 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d053 OWNER TO cbs_uge_group;

--
-- Name: d054; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d054 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d054 OWNER TO cbs_uge_group;

--
-- Name: d055; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d055 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d055 OWNER TO cbs_uge_group;

--
-- Name: d056; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d056 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d056 OWNER TO cbs_uge_group;

--
-- Name: d057; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d057 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d057 OWNER TO cbs_uge_group;

--
-- Name: d058; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d058 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d058 OWNER TO cbs_uge_group;

--
-- Name: d059; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d059 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d059 OWNER TO cbs_uge_group;

--
-- Name: d060; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d060 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d060 OWNER TO cbs_uge_group;

--
-- Name: d061; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d061 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d061 OWNER TO cbs_uge_group;

--
-- Name: d062; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d062 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d062 OWNER TO cbs_uge_group;

--
-- Name: d063; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d063 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d063 OWNER TO cbs_uge_group;

--
-- Name: d064; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d064 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d064 OWNER TO cbs_uge_group;

--
-- Name: d065; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d065 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d065 OWNER TO cbs_uge_group;

--
-- Name: d066; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d066 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d066 OWNER TO cbs_uge_group;

--
-- Name: d067; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d067 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d067 OWNER TO cbs_uge_group;

--
-- Name: d068; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d068 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d068 OWNER TO cbs_uge_group;

--
-- Name: d069; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d069 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d069 OWNER TO cbs_uge_group;

--
-- Name: d070; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d070 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d070 OWNER TO cbs_uge_group;

--
-- Name: d071; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d071 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d071 OWNER TO cbs_uge_group;

--
-- Name: d072; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d072 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d072 OWNER TO cbs_uge_group;

--
-- Name: d073; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d073 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d073 OWNER TO cbs_uge_group;

--
-- Name: d074; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d074 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d074 OWNER TO cbs_uge_group;

--
-- Name: d075; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d075 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d075 OWNER TO cbs_uge_group;

--
-- Name: d076; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d076 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d076 OWNER TO cbs_uge_group;

--
-- Name: d077; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d077 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d077 OWNER TO cbs_uge_group;

--
-- Name: d078; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d078 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d078 OWNER TO cbs_uge_group;

--
-- Name: d079; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d079 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d079 OWNER TO cbs_uge_group;

--
-- Name: d080; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d080 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d080 OWNER TO cbs_uge_group;

--
-- Name: d081; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d081 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d081 OWNER TO cbs_uge_group;

--
-- Name: d082; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d082 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d082 OWNER TO cbs_uge_group;

--
-- Name: d083; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d083 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d083 OWNER TO cbs_uge_group;

--
-- Name: d084; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d084 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d084 OWNER TO cbs_uge_group;

--
-- Name: d085; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d085 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d085 OWNER TO cbs_uge_group;

--
-- Name: d086; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d086 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d086 OWNER TO cbs_uge_group;

--
-- Name: d087; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d087 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d087 OWNER TO cbs_uge_group;

--
-- Name: d088; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d088 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d088 OWNER TO cbs_uge_group;

--
-- Name: d089; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d089 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d089 OWNER TO cbs_uge_group;

--
-- Name: d090; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d090 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d090 OWNER TO cbs_uge_group;

--
-- Name: d091; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d091 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d091 OWNER TO cbs_uge_group;

--
-- Name: d092; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d092 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d092 OWNER TO cbs_uge_group;

--
-- Name: d093; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d093 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d093 OWNER TO cbs_uge_group;

--
-- Name: d094; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d094 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d094 OWNER TO cbs_uge_group;

--
-- Name: d095; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d095 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2154)
);


ALTER TABLE bd_alti.d095 OWNER TO cbs_uge_group;

--
-- Name: d971; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d971 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,5490)
);


ALTER TABLE bd_alti.d971 OWNER TO cbs_uge_group;

--
-- Name: d972; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d972 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,5490)
);


ALTER TABLE bd_alti.d972 OWNER TO cbs_uge_group;

--
-- Name: d973; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d973 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2972)
);


ALTER TABLE bd_alti.d973 OWNER TO cbs_uge_group;

--
-- Name: d974; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d974 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ,2975)
);


ALTER TABLE bd_alti.d974 OWNER TO cbs_uge_group;

--
-- Name: d976; Type: TABLE; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE TABLE bd_alti.d976 (
    id integer NOT NULL,
    the_geom public.geometry(PointZ)
);


ALTER TABLE bd_alti.d976 OWNER TO cbs_uge_group;

--
-- Name: d001 pk_d001; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d001
    ADD CONSTRAINT pk_d001 PRIMARY KEY (id);


--
-- Name: d002 pk_d002; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d002
    ADD CONSTRAINT pk_d002 PRIMARY KEY (id);


--
-- Name: d003 pk_d003; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d003
    ADD CONSTRAINT pk_d003 PRIMARY KEY (id);


--
-- Name: d004 pk_d004; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d004
    ADD CONSTRAINT pk_d004 PRIMARY KEY (id);


--
-- Name: d005 pk_d005; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d005
    ADD CONSTRAINT pk_d005 PRIMARY KEY (id);


--
-- Name: d006 pk_d006; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d006
    ADD CONSTRAINT pk_d006 PRIMARY KEY (id);


--
-- Name: d007 pk_d007; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d007
    ADD CONSTRAINT pk_d007 PRIMARY KEY (id);


--
-- Name: d008 pk_d008; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d008
    ADD CONSTRAINT pk_d008 PRIMARY KEY (id);


--
-- Name: d009 pk_d009; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d009
    ADD CONSTRAINT pk_d009 PRIMARY KEY (id);


--
-- Name: d010 pk_d010; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d010
    ADD CONSTRAINT pk_d010 PRIMARY KEY (id);


--
-- Name: d011 pk_d011; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d011
    ADD CONSTRAINT pk_d011 PRIMARY KEY (id);


--
-- Name: d012 pk_d012; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d012
    ADD CONSTRAINT pk_d012 PRIMARY KEY (id);


--
-- Name: d013 pk_d013; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d013
    ADD CONSTRAINT pk_d013 PRIMARY KEY (id);


--
-- Name: d014 pk_d014; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d014
    ADD CONSTRAINT pk_d014 PRIMARY KEY (id);


--
-- Name: d015 pk_d015; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d015
    ADD CONSTRAINT pk_d015 PRIMARY KEY (id);


--
-- Name: d016 pk_d016; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d016
    ADD CONSTRAINT pk_d016 PRIMARY KEY (id);


--
-- Name: d017 pk_d017; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d017
    ADD CONSTRAINT pk_d017 PRIMARY KEY (id);


--
-- Name: d018 pk_d018; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d018
    ADD CONSTRAINT pk_d018 PRIMARY KEY (id);


--
-- Name: d019 pk_d019; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d019
    ADD CONSTRAINT pk_d019 PRIMARY KEY (id);


--
-- Name: d021 pk_d021; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d021
    ADD CONSTRAINT pk_d021 PRIMARY KEY (id);


--
-- Name: d022 pk_d022; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d022
    ADD CONSTRAINT pk_d022 PRIMARY KEY (id);


--
-- Name: d023 pk_d023; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d023
    ADD CONSTRAINT pk_d023 PRIMARY KEY (id);


--
-- Name: d024 pk_d024; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d024
    ADD CONSTRAINT pk_d024 PRIMARY KEY (id);


--
-- Name: d025 pk_d025; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d025
    ADD CONSTRAINT pk_d025 PRIMARY KEY (id);


--
-- Name: d026 pk_d026; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d026
    ADD CONSTRAINT pk_d026 PRIMARY KEY (id);


--
-- Name: d027 pk_d027; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d027
    ADD CONSTRAINT pk_d027 PRIMARY KEY (id);


--
-- Name: d028 pk_d028; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d028
    ADD CONSTRAINT pk_d028 PRIMARY KEY (id);


--
-- Name: d029 pk_d029; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d029
    ADD CONSTRAINT pk_d029 PRIMARY KEY (id);


--
-- Name: d02a pk_d02a; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d02a
    ADD CONSTRAINT pk_d02a PRIMARY KEY (id);


--
-- Name: d02b pk_d02b; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d02b
    ADD CONSTRAINT pk_d02b PRIMARY KEY (id);


--
-- Name: d030 pk_d030; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d030
    ADD CONSTRAINT pk_d030 PRIMARY KEY (id);


--
-- Name: d031 pk_d031; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d031
    ADD CONSTRAINT pk_d031 PRIMARY KEY (id);


--
-- Name: d032 pk_d032; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d032
    ADD CONSTRAINT pk_d032 PRIMARY KEY (id);


--
-- Name: d033 pk_d033; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d033
    ADD CONSTRAINT pk_d033 PRIMARY KEY (id);


--
-- Name: d034 pk_d034; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d034
    ADD CONSTRAINT pk_d034 PRIMARY KEY (id);


--
-- Name: d035 pk_d035; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d035
    ADD CONSTRAINT pk_d035 PRIMARY KEY (id);


--
-- Name: d036 pk_d036; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d036
    ADD CONSTRAINT pk_d036 PRIMARY KEY (id);


--
-- Name: d037 pk_d037; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d037
    ADD CONSTRAINT pk_d037 PRIMARY KEY (id);


--
-- Name: d038 pk_d038; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d038
    ADD CONSTRAINT pk_d038 PRIMARY KEY (id);


--
-- Name: d039 pk_d039; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d039
    ADD CONSTRAINT pk_d039 PRIMARY KEY (id);


--
-- Name: d040 pk_d040; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d040
    ADD CONSTRAINT pk_d040 PRIMARY KEY (id);


--
-- Name: d041 pk_d041; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d041
    ADD CONSTRAINT pk_d041 PRIMARY KEY (id);


--
-- Name: d042 pk_d042; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d042
    ADD CONSTRAINT pk_d042 PRIMARY KEY (id);


--
-- Name: d043 pk_d043; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d043
    ADD CONSTRAINT pk_d043 PRIMARY KEY (id);


--
-- Name: d044 pk_d044; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d044
    ADD CONSTRAINT pk_d044 PRIMARY KEY (id);


--
-- Name: d045 pk_d045; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d045
    ADD CONSTRAINT pk_d045 PRIMARY KEY (id);


--
-- Name: d046 pk_d046; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d046
    ADD CONSTRAINT pk_d046 PRIMARY KEY (id);


--
-- Name: d047 pk_d047; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d047
    ADD CONSTRAINT pk_d047 PRIMARY KEY (id);


--
-- Name: d048 pk_d048; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d048
    ADD CONSTRAINT pk_d048 PRIMARY KEY (id);


--
-- Name: d049 pk_d049; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d049
    ADD CONSTRAINT pk_d049 PRIMARY KEY (id);


--
-- Name: d050 pk_d050; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d050
    ADD CONSTRAINT pk_d050 PRIMARY KEY (id);


--
-- Name: d051 pk_d051; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d051
    ADD CONSTRAINT pk_d051 PRIMARY KEY (id);


--
-- Name: d052 pk_d052; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d052
    ADD CONSTRAINT pk_d052 PRIMARY KEY (id);


--
-- Name: d053 pk_d053; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d053
    ADD CONSTRAINT pk_d053 PRIMARY KEY (id);


--
-- Name: d054 pk_d054; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d054
    ADD CONSTRAINT pk_d054 PRIMARY KEY (id);


--
-- Name: d055 pk_d055; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d055
    ADD CONSTRAINT pk_d055 PRIMARY KEY (id);


--
-- Name: d056 pk_d056; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d056
    ADD CONSTRAINT pk_d056 PRIMARY KEY (id);


--
-- Name: d057 pk_d057; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d057
    ADD CONSTRAINT pk_d057 PRIMARY KEY (id);


--
-- Name: d058 pk_d058; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d058
    ADD CONSTRAINT pk_d058 PRIMARY KEY (id);


--
-- Name: d059 pk_d059; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d059
    ADD CONSTRAINT pk_d059 PRIMARY KEY (id);


--
-- Name: d060 pk_d060; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d060
    ADD CONSTRAINT pk_d060 PRIMARY KEY (id);


--
-- Name: d061 pk_d061; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d061
    ADD CONSTRAINT pk_d061 PRIMARY KEY (id);


--
-- Name: d062 pk_d062; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d062
    ADD CONSTRAINT pk_d062 PRIMARY KEY (id);


--
-- Name: d063 pk_d063; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d063
    ADD CONSTRAINT pk_d063 PRIMARY KEY (id);


--
-- Name: d064 pk_d064; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d064
    ADD CONSTRAINT pk_d064 PRIMARY KEY (id);


--
-- Name: d065 pk_d065; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d065
    ADD CONSTRAINT pk_d065 PRIMARY KEY (id);


--
-- Name: d066 pk_d066; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d066
    ADD CONSTRAINT pk_d066 PRIMARY KEY (id);


--
-- Name: d067 pk_d067; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d067
    ADD CONSTRAINT pk_d067 PRIMARY KEY (id);


--
-- Name: d068 pk_d068; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d068
    ADD CONSTRAINT pk_d068 PRIMARY KEY (id);


--
-- Name: d069 pk_d069; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d069
    ADD CONSTRAINT pk_d069 PRIMARY KEY (id);


--
-- Name: d070 pk_d070; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d070
    ADD CONSTRAINT pk_d070 PRIMARY KEY (id);


--
-- Name: d071 pk_d071; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d071
    ADD CONSTRAINT pk_d071 PRIMARY KEY (id);


--
-- Name: d072 pk_d072; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d072
    ADD CONSTRAINT pk_d072 PRIMARY KEY (id);


--
-- Name: d073 pk_d073; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d073
    ADD CONSTRAINT pk_d073 PRIMARY KEY (id);


--
-- Name: d074 pk_d074; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d074
    ADD CONSTRAINT pk_d074 PRIMARY KEY (id);


--
-- Name: d075 pk_d075; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d075
    ADD CONSTRAINT pk_d075 PRIMARY KEY (id);


--
-- Name: d076 pk_d076; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d076
    ADD CONSTRAINT pk_d076 PRIMARY KEY (id);


--
-- Name: d077 pk_d077; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d077
    ADD CONSTRAINT pk_d077 PRIMARY KEY (id);


--
-- Name: d078 pk_d078; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d078
    ADD CONSTRAINT pk_d078 PRIMARY KEY (id);


--
-- Name: d079 pk_d079; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d079
    ADD CONSTRAINT pk_d079 PRIMARY KEY (id);


--
-- Name: d080 pk_d080; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d080
    ADD CONSTRAINT pk_d080 PRIMARY KEY (id);


--
-- Name: d081 pk_d081; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d081
    ADD CONSTRAINT pk_d081 PRIMARY KEY (id);


--
-- Name: d082 pk_d082; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d082
    ADD CONSTRAINT pk_d082 PRIMARY KEY (id);


--
-- Name: d083 pk_d083; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d083
    ADD CONSTRAINT pk_d083 PRIMARY KEY (id);


--
-- Name: d084 pk_d084; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d084
    ADD CONSTRAINT pk_d084 PRIMARY KEY (id);


--
-- Name: d085 pk_d085; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d085
    ADD CONSTRAINT pk_d085 PRIMARY KEY (id);


--
-- Name: d086 pk_d086; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d086
    ADD CONSTRAINT pk_d086 PRIMARY KEY (id);


--
-- Name: d087 pk_d087; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d087
    ADD CONSTRAINT pk_d087 PRIMARY KEY (id);


--
-- Name: d088 pk_d088; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d088
    ADD CONSTRAINT pk_d088 PRIMARY KEY (id);


--
-- Name: d089 pk_d089; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d089
    ADD CONSTRAINT pk_d089 PRIMARY KEY (id);


--
-- Name: d090 pk_d090; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d090
    ADD CONSTRAINT pk_d090 PRIMARY KEY (id);


--
-- Name: d091 pk_d091; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d091
    ADD CONSTRAINT pk_d091 PRIMARY KEY (id);


--
-- Name: d092 pk_d092; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d092
    ADD CONSTRAINT pk_d092 PRIMARY KEY (id);


--
-- Name: d093 pk_d093; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d093
    ADD CONSTRAINT pk_d093 PRIMARY KEY (id);


--
-- Name: d094 pk_d094; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d094
    ADD CONSTRAINT pk_d094 PRIMARY KEY (id);


--
-- Name: d095 pk_d095; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d095
    ADD CONSTRAINT pk_d095 PRIMARY KEY (id);


--
-- Name: d971 pk_d971; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d971
    ADD CONSTRAINT pk_d971 PRIMARY KEY (id);


--
-- Name: d972 pk_d972; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d972
    ADD CONSTRAINT pk_d972 PRIMARY KEY (id);


--
-- Name: d973 pk_d973; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d973
    ADD CONSTRAINT pk_d973 PRIMARY KEY (id);


--
-- Name: d974 pk_d974; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d974
    ADD CONSTRAINT pk_d974 PRIMARY KEY (id);


--
-- Name: d976 pk_d976; Type: CONSTRAINT; Schema: bd_alti; Owner: cbs_uge_group
--

ALTER TABLE ONLY bd_alti.d976
    ADD CONSTRAINT pk_d976 PRIMARY KEY (id);


--
-- Name: geom_idx_d001; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d001 ON bd_alti.d001 USING gist (the_geom);


--
-- Name: geom_idx_d002; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d002 ON bd_alti.d002 USING gist (the_geom);


--
-- Name: geom_idx_d003; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d003 ON bd_alti.d003 USING gist (the_geom);


--
-- Name: geom_idx_d004; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d004 ON bd_alti.d004 USING gist (the_geom);


--
-- Name: geom_idx_d005; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d005 ON bd_alti.d005 USING gist (the_geom);


--
-- Name: geom_idx_d006; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d006 ON bd_alti.d006 USING gist (the_geom);


--
-- Name: geom_idx_d007; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d007 ON bd_alti.d007 USING gist (the_geom);


--
-- Name: geom_idx_d008; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d008 ON bd_alti.d008 USING gist (the_geom);


--
-- Name: geom_idx_d009; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d009 ON bd_alti.d009 USING gist (the_geom);


--
-- Name: geom_idx_d010; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d010 ON bd_alti.d010 USING gist (the_geom);


--
-- Name: geom_idx_d011; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d011 ON bd_alti.d011 USING gist (the_geom);


--
-- Name: geom_idx_d012; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d012 ON bd_alti.d012 USING gist (the_geom);


--
-- Name: geom_idx_d013; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d013 ON bd_alti.d013 USING gist (the_geom);


--
-- Name: geom_idx_d014; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d014 ON bd_alti.d014 USING gist (the_geom);


--
-- Name: geom_idx_d015; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d015 ON bd_alti.d015 USING gist (the_geom);


--
-- Name: geom_idx_d016; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d016 ON bd_alti.d016 USING gist (the_geom);


--
-- Name: geom_idx_d017; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d017 ON bd_alti.d017 USING gist (the_geom);


--
-- Name: geom_idx_d018; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d018 ON bd_alti.d018 USING gist (the_geom);


--
-- Name: geom_idx_d019; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d019 ON bd_alti.d019 USING gist (the_geom);


--
-- Name: geom_idx_d021; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d021 ON bd_alti.d021 USING gist (the_geom);


--
-- Name: geom_idx_d022; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d022 ON bd_alti.d022 USING gist (the_geom);


--
-- Name: geom_idx_d023; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d023 ON bd_alti.d023 USING gist (the_geom);


--
-- Name: geom_idx_d024; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d024 ON bd_alti.d024 USING gist (the_geom);


--
-- Name: geom_idx_d025; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d025 ON bd_alti.d025 USING gist (the_geom);


--
-- Name: geom_idx_d026; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d026 ON bd_alti.d026 USING gist (the_geom);


--
-- Name: geom_idx_d027; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d027 ON bd_alti.d027 USING gist (the_geom);


--
-- Name: geom_idx_d028; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d028 ON bd_alti.d028 USING gist (the_geom);


--
-- Name: geom_idx_d029; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d029 ON bd_alti.d029 USING gist (the_geom);


--
-- Name: geom_idx_d02a; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d02a ON bd_alti.d02a USING gist (the_geom);


--
-- Name: geom_idx_d02b; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d02b ON bd_alti.d02b USING gist (the_geom);


--
-- Name: geom_idx_d030; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d030 ON bd_alti.d030 USING gist (the_geom);


--
-- Name: geom_idx_d031; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d031 ON bd_alti.d031 USING gist (the_geom);


--
-- Name: geom_idx_d032; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d032 ON bd_alti.d032 USING gist (the_geom);


--
-- Name: geom_idx_d033; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d033 ON bd_alti.d033 USING gist (the_geom);


--
-- Name: geom_idx_d034; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d034 ON bd_alti.d034 USING gist (the_geom);


--
-- Name: geom_idx_d035; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d035 ON bd_alti.d035 USING gist (the_geom);


--
-- Name: geom_idx_d036; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d036 ON bd_alti.d036 USING gist (the_geom);


--
-- Name: geom_idx_d037; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d037 ON bd_alti.d037 USING gist (the_geom);


--
-- Name: geom_idx_d038; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d038 ON bd_alti.d038 USING gist (the_geom);


--
-- Name: geom_idx_d039; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d039 ON bd_alti.d039 USING gist (the_geom);


--
-- Name: geom_idx_d040; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d040 ON bd_alti.d040 USING gist (the_geom);


--
-- Name: geom_idx_d041; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d041 ON bd_alti.d041 USING gist (the_geom);


--
-- Name: geom_idx_d042; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d042 ON bd_alti.d042 USING gist (the_geom);


--
-- Name: geom_idx_d043; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d043 ON bd_alti.d043 USING gist (the_geom);


--
-- Name: geom_idx_d044; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d044 ON bd_alti.d044 USING gist (the_geom);


--
-- Name: geom_idx_d045; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d045 ON bd_alti.d045 USING gist (the_geom);


--
-- Name: geom_idx_d046; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d046 ON bd_alti.d046 USING gist (the_geom);


--
-- Name: geom_idx_d047; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d047 ON bd_alti.d047 USING gist (the_geom);


--
-- Name: geom_idx_d048; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d048 ON bd_alti.d048 USING gist (the_geom);


--
-- Name: geom_idx_d049; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d049 ON bd_alti.d049 USING gist (the_geom);


--
-- Name: geom_idx_d050; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d050 ON bd_alti.d050 USING gist (the_geom);


--
-- Name: geom_idx_d051; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d051 ON bd_alti.d051 USING gist (the_geom);


--
-- Name: geom_idx_d052; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d052 ON bd_alti.d052 USING gist (the_geom);


--
-- Name: geom_idx_d053; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d053 ON bd_alti.d053 USING gist (the_geom);


--
-- Name: geom_idx_d054; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d054 ON bd_alti.d054 USING gist (the_geom);


--
-- Name: geom_idx_d055; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d055 ON bd_alti.d055 USING gist (the_geom);


--
-- Name: geom_idx_d056; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d056 ON bd_alti.d056 USING gist (the_geom);


--
-- Name: geom_idx_d057; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d057 ON bd_alti.d057 USING gist (the_geom);


--
-- Name: geom_idx_d058; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d058 ON bd_alti.d058 USING gist (the_geom);


--
-- Name: geom_idx_d059; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d059 ON bd_alti.d059 USING gist (the_geom);


--
-- Name: geom_idx_d060; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d060 ON bd_alti.d060 USING gist (the_geom);


--
-- Name: geom_idx_d061; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d061 ON bd_alti.d061 USING gist (the_geom);


--
-- Name: geom_idx_d062; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d062 ON bd_alti.d062 USING gist (the_geom);


--
-- Name: geom_idx_d063; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d063 ON bd_alti.d063 USING gist (the_geom);


--
-- Name: geom_idx_d064; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d064 ON bd_alti.d064 USING gist (the_geom);


--
-- Name: geom_idx_d065; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d065 ON bd_alti.d065 USING gist (the_geom);


--
-- Name: geom_idx_d066; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d066 ON bd_alti.d066 USING gist (the_geom);


--
-- Name: geom_idx_d067; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d067 ON bd_alti.d067 USING gist (the_geom);


--
-- Name: geom_idx_d068; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d068 ON bd_alti.d068 USING gist (the_geom);


--
-- Name: geom_idx_d069; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d069 ON bd_alti.d069 USING gist (the_geom);


--
-- Name: geom_idx_d070; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d070 ON bd_alti.d070 USING gist (the_geom);


--
-- Name: geom_idx_d071; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d071 ON bd_alti.d071 USING gist (the_geom);


--
-- Name: geom_idx_d072; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d072 ON bd_alti.d072 USING gist (the_geom);


--
-- Name: geom_idx_d073; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d073 ON bd_alti.d073 USING gist (the_geom);


--
-- Name: geom_idx_d074; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d074 ON bd_alti.d074 USING gist (the_geom);


--
-- Name: geom_idx_d075; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d075 ON bd_alti.d075 USING gist (the_geom);


--
-- Name: geom_idx_d076; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d076 ON bd_alti.d076 USING gist (the_geom);


--
-- Name: geom_idx_d077; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d077 ON bd_alti.d077 USING gist (the_geom);


--
-- Name: geom_idx_d078; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d078 ON bd_alti.d078 USING gist (the_geom);


--
-- Name: geom_idx_d079; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d079 ON bd_alti.d079 USING gist (the_geom);


--
-- Name: geom_idx_d080; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d080 ON bd_alti.d080 USING gist (the_geom);


--
-- Name: geom_idx_d081; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d081 ON bd_alti.d081 USING gist (the_geom);


--
-- Name: geom_idx_d082; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d082 ON bd_alti.d082 USING gist (the_geom);


--
-- Name: geom_idx_d083; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d083 ON bd_alti.d083 USING gist (the_geom);


--
-- Name: geom_idx_d084; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d084 ON bd_alti.d084 USING gist (the_geom);


--
-- Name: geom_idx_d085; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d085 ON bd_alti.d085 USING gist (the_geom);


--
-- Name: geom_idx_d086; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d086 ON bd_alti.d086 USING gist (the_geom);


--
-- Name: geom_idx_d087; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d087 ON bd_alti.d087 USING gist (the_geom);


--
-- Name: geom_idx_d088; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d088 ON bd_alti.d088 USING gist (the_geom);


--
-- Name: geom_idx_d089; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d089 ON bd_alti.d089 USING gist (the_geom);


--
-- Name: geom_idx_d090; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d090 ON bd_alti.d090 USING gist (the_geom);


--
-- Name: geom_idx_d091; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d091 ON bd_alti.d091 USING gist (the_geom);


--
-- Name: geom_idx_d092; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d092 ON bd_alti.d092 USING gist (the_geom);


--
-- Name: geom_idx_d093; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d093 ON bd_alti.d093 USING gist (the_geom);


--
-- Name: geom_idx_d094; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d094 ON bd_alti.d094 USING gist (the_geom);


--
-- Name: geom_idx_d095; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d095 ON bd_alti.d095 USING gist (the_geom);


--
-- Name: geom_idx_d971; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d971 ON bd_alti.d971 USING gist (the_geom);


--
-- Name: geom_idx_d972; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d972 ON bd_alti.d972 USING gist (the_geom);


--
-- Name: geom_idx_d973; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d973 ON bd_alti.d973 USING gist (the_geom);


--
-- Name: geom_idx_d974; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d974 ON bd_alti.d974 USING gist (the_geom);


--
-- Name: geom_idx_d976; Type: INDEX; Schema: bd_alti; Owner: cbs_uge_group
--

CREATE INDEX geom_idx_d976 ON bd_alti.d976 USING gist (the_geom);

CREATE TABLE bd_topo.n_ligne_orographique_bdt_000_2023 (
	geom3d public.geometry(linestringz, 2154) NULL,
	cleabs varchar(24) NOT NULL,
	nature varchar NULL,
	geom public.geometry(linestring, 2154) NULL,
	CONSTRAINT n_ligne_orographique_bdt_000_2023_pkey PRIMARY KEY (cleabs)
);

CREATE TABLE bd_topo.n_troncon_hydrographique_bdt_000_2023 (
	geom3d public.geometry(linestringz, 2154) NULL,
	cleabs varchar(24) NOT NULL,
	nature varchar NULL,
	fictif bool NULL,
	position_par_rapport_au_sol varchar NULL,
	persistance varchar NULL,
	fosse bool NULL,
	geom public.geometry(linestring, 2154) NULL,
	CONSTRAINT n_troncon_hydrographique_bdt_000_2023_pkey PRIMARY KEY (cleabs)
);
