\connect postgres
-- Database: cammesa_db
-- DROP DATABASE IF EXISTS cammesa_db;
CREATE DATABASE cammesa_db
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Argentina.1252'
    LC_CTYPE = 'Spanish_Argentina.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE SCHEMA IF NOT EXISTS cammesa_db;

CREATE TABLE IF NOT EXISTS cammesa_db.regions (
    region_id		integer PRIMARY KEY
	, region_code	integer	NOT NULL
	, region_desc	varchar(40)	NOT NULL
	, parent_id		integer
	, create_user	char(10)
	, create_date	timestamp
	, update_user	char(10)
	, update_date	timestamp
);

CREATE TABLE IF NOT EXISTS cammesa_db.hourly_demand (
	region_code		integer		NOT NULL
	, timestamp		timestamp 	NOT NULL
	, hourly_demand	integer 	NOT NULL
	, hourly_temp	float 		NOT NULL
	, day_of_week	integer		NOT NULL
	, is_holiday	integer		NOT NULL
	, create_user	char(10)
	, create_date	timestamp
	, update_user	char(10)
	, update_date	timestamp,
	CONSTRAINT hourly_demand_PK
    PRIMARY KEY (region_code, timestamp)
);

-- Table: cammesa_db.hourly_demand_forecast
-- DROP TABLE IF EXISTS cammesa_db.hourly_demand_forecast;
CREATE TABLE IF NOT EXISTS cammesa_db.hourly_demand_forecast
(
    region_code integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    hourly_demand_forecast integer NOT NULL,
    hourly_temp_forecast double precision,
    day_of_week integer NOT NULL,
    is_holiday integer NOT NULL,
    create_user character(10) COLLATE pg_catalog."default",
    create_date timestamp without time zone,
    update_user character(10) COLLATE pg_catalog."default",
    update_date timestamp without time zone,
    CONSTRAINT hourly_demand_forecast_pk PRIMARY KEY (region_code, "timestamp")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.hourly_demand_forecast
    OWNER to postgres;

-- Table: cammesa_db.tariffs
-- DROP TABLE IF EXISTS cammesa_db.tariffs;
CREATE TABLE IF NOT EXISTS cammesa_db.tariffs
(
    tariff_id 		character(10) 	NOT NULL,
	tariff_desc		character(100) 	NOT NULL,
	tariff_categ	character(50) 	NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT tariffs_pk PRIMARY KEY (tariff_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.tariffs
    OWNER to postgres;

-- Table: cammesa_db.agents
-- DROP TABLE IF EXISTS cammesa_db.agents;
CREATE TABLE IF NOT EXISTS cammesa_db.agents
(
    agent_id 		character(8) 	NOT NULL,
	agent_desc		character(100) 	NOT NULL,
	agent_dem_type	character(2),
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT agents_pk PRIMARY KEY (agent_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.agents
    OWNER to postgres;

-- Table: cammesa_db.gen_technologies
--DROP TABLE IF EXISTS cammesa_db.gen_technologies;
CREATE TABLE IF NOT EXISTS cammesa_db.gen_technologies
(
    technology 		character(4) 	NOT NULL,
    technology_desc	character(20) 	NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT gen_technologies_pk PRIMARY KEY (technology)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.gen_technologies
    OWNER to postgres;

-- Table: cammesa_db.gen_machines
--DROP TABLE IF EXISTS cammesa_db.gen_machines;
CREATE TABLE IF NOT EXISTS cammesa_db.gen_machines
(
    machine_id 		character(10) 	NOT NULL,
    machine_code	character(8) 	NOT NULL,
	machine_type	character(20) 	NULL,
	source_gen		character(10) 	NULL,
	technology		character(4)	NULL,
	hidraulic_categ	character(30)	NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT gen_machines_pk PRIMARY KEY (machine_id),
    CONSTRAINT gen_machines_gen_technologies_fk FOREIGN KEY(technology) 
		REFERENCES cammesa_db.gen_technologies(technology),

)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.gen_machines
    OWNER to postgres;

-- Table: cammesa_db.monthly_demand
-- DROP TABLE IF EXISTS cammesa_db.monthly_demand
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_demand
(
	month			date			NOT NULL,
	agent_id 		character(8) 	NOT NULL,
	region_desc		character(20) 	NOT NULL,
	prov_desc		character(20) 	NOT NULL,
    year			int				NOT NULL,
	area_categ		character(20) 	NOT NULL,
	demand_categ	character(20) 	NOT NULL,
	tariff_id		character(10)	NOT NULL,
	monthly_demand_mwh	double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_demand_pk PRIMARY KEY (month, agent_id, region_desc, tariff_id),
	CONSTRAINT monthly_demand_tariffs_fk FOREIGN KEY(tariff_id) 
		REFERENCES cammesa_db.tariffs(tariff_id),
	CONSTRAINT monthly_demand_agents_fk FOREIGN KEY(agent_id) 
		REFERENCES cammesa_db.agents(agent_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.monthly_demand
    OWNER to postgres;

-- Table: cammesa_db.monthly_gen
--DROP TABLE IF EXISTS cammesa_db.monthly_gen;
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_gen
(
	month			date			NOT NULL,
	machine_id		character(10)	NOT NULL,
	agent_id 		character(8) 	NOT NULL,
	central_id 		character(8) 	NOT NULL,
	region_desc		character(20) 	NOT NULL,
	region_categ	character(30) 	NOT NULL,
	prov_desc		character(20) 	NULL,
    year			int				NOT NULL,
	monthly_gen_mwh	double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_gen_pk PRIMARY KEY (month, machine_id, agent_id),
	CONSTRAINT monthly_gen_machines_fk FOREIGN KEY(machine_id) 
		REFERENCES cammesa_db.gen_machines(machine_id),
	CONSTRAINT monthly_gen_agents_fk FOREIGN KEY(agent_id) 
		REFERENCES cammesa_db.agents(agent_id)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.monthly_gen
    OWNER to postgres;

-- Table: cammesa_db.monthly_prices
--DROP TABLE IF EXISTS cammesa_db.monthly_prices;
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_prices
(
	month			date				NOT NULL,
	energia			double precision	NOT NULL,
	energia_ad		double precision	NOT NULL,
	sobrecost_comb	double precision	NULL,
	sobrecost_transit_despacho	double precision NULL,
	carg_dem_exce_cta_brasil_contrat_abas_MEM	double precision NULL,
	pot_despachada	double precision	NULL,
	pot_serv_asoc	double precision	NULL,
	pot_res_corto_plzo_serv_res_intantanea	double precision NULL,
	pot_res_med_plzo	double precision NULL,
	monodico		double precision	NOT NULL,
	transp_alta_tens_distrib_troncal	double precision NULL,
	transp_alta_tens	double precision NULL,
	transp_distrib_troncal	double precision	NULL,
	monodico_transp	double precision	NULL,
	monodico_ponder_estacional_otr_ingr	double precision NULL,
	cargo_demanda_exced_real	double precision NULL,
	cta_brasil_abast_MEM	double precision NULL,
	compra_conj_MEM			double precision NULL,
	monodico_ponder_estacional_transp	double precision NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_prices_pk PRIMARY KEY (month)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.monthly_prices
    OWNER to postgres;

-- Table: cammesa_db.monthly_combustibles
--DROP TABLE IF EXISTS cammesa_db.monthly_combustibles;
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_combustibles
(
	month			date			NOT NULL,
	machine_id		character(10)	NOT NULL,
	agent_id 		character(8) 	NOT NULL,
	central_id 		character(8) 	NOT NULL,
    year			int				NOT NULL,
	combustible_type character(15) 	NOT NULL,
	monthly_consume	double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_combustibles_pk PRIMARY KEY (month, machine_id, agent_id, combustible_type),
	CONSTRAINT monthly_combustibles_machines_fk FOREIGN KEY(machine_id) 
		REFERENCES cammesa_db.gen_machines(machine_id),
	CONSTRAINT monthly_combustibles_agents_fk FOREIGN KEY(agent_id) 
		REFERENCES cammesa_db.agents(agent_id)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.monthly_combustibles
    OWNER to postgres;

-- Table: cammesa_db.monthly_availability
--DROP TABLE IF EXISTS cammesa_db.monthly_availability;
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_availability
(
	month			date			NOT NULL,
	central_id 		character(8) 	NOT NULL,
	agent_id 		character(8) 	NOT NULL,
    year			int				NOT NULL,
	technology		character(4) 	NOT NULL,
	monthly_availability_factor	double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_availability_pk PRIMARY KEY (month, central_id, agent_id, technology),
	CONSTRAINT monthly_availability_gen_technologies_fk FOREIGN KEY(technology) 
		REFERENCES cammesa_db.gen_technologies(technology),
	CONSTRAINT monthly_combustibles_agents_fk FOREIGN KEY(agent_id) 
		REFERENCES cammesa_db.agents(agent_id)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.monthly_availability
    OWNER to postgres;

-- Table: cammesa_db.monthly_import_export
--DROP TABLE IF EXISTS cammesa_db.monthly_import_export
CREATE TABLE IF NOT EXISTS cammesa_db.monthly_import_export
(
	month			date			NOT NULL,
	pais     		character(10) 	NOT NULL,
    year			int				NOT NULL,
	import_export_type	character(11) 	NOT NULL,
	monthly_energy_mwh	double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT monthly_import_export_pk PRIMARY KEY (month, pais, import_export_type)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.monthly_import_export
    OWNER to postgres;

-- Table: cammesa_db.experiments
-- DROP TABLE IF EXISTS cammesa_db.experiments
CREATE TABLE IF NOT EXISTS cammesa_db.experiments
(
	experiment_id   character(10) 	NOT NULL,
    experiment_date	date			NOT NULL,
	model_id 		character(10) 	NOT NULL,
	mape	        double precision NOT NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT experiments_pk PRIMARY KEY (experiment_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.experiments
    OWNER to postgres;

-- Table: cammesa_db.experiments
-- DROP TABLE IF EXISTS cammesa_db.experiments_detail
CREATE TABLE IF NOT EXISTS cammesa_db.experiments_detail
(
	experiment_id   character(10) 	NOT NULL,
    month			date			NOT NULL,
	hist_value      double precision NULL,
	forecast_value  double precision NULL,
    create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT experiments_detail_pk PRIMARY KEY (experiment_id, month),
	CONSTRAINT experiments_detail_experiments_fk FOREIGN KEY(experiment_id)
		REFERENCES cammesa_db.experiments(experiment_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS cammesa_db.experiments_detail
    OWNER to postgres;

-- Table: cammesa_db.hourly_historic_demand
--DROP TABLE IF EXISTS cammesa_db.hourly_historic_demand
CREATE TABLE IF NOT EXISTS cammesa_db.hourly_historic_demand
(
	timestamp		timestamp without time zone	NOT NULL,
	temp     		double precision	NOT NULL,
    humidity		int					NOT NULL,
	press			double precision	NOT NULL,
    dd				int					NOT NULL,
    wind			int					NOT NULL,
	met_station		character(50)	 	NOT NULL,
	create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT hourly_historic_demand_pk PRIMARY KEY (timestamp, met_station)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.hourly_historic_demand
    OWNER to postgres;

-- Table: cammesa_db.hourly_historic_demand
--DROP TABLE IF EXISTS cammesa_db.hourly_historic_demand
CREATE TABLE IF NOT EXISTS cammesa_db.hourly_historic_demand
(
	timestamp				timestamp without time zone	NOT NULL,
	day_type				character(8)	 	NOT NULL,
	hourly_local_demand_mwh	int					NOT NULL,
    hourly_total_demand_mwh	int					NOT NULL,
	GU_MEM					int					NOT NULL,
    distrib					int					NOT NULL,
    expo					int					NOT NULL,
    pumped					int					NOT NULL,
    losses					int					NOT NULL,
	create_user 	character(10) COLLATE pg_catalog."default",
    create_date 	timestamp without time zone,
    update_user 	character(10) COLLATE pg_catalog."default",
    update_date 	timestamp without time zone,
    CONSTRAINT hourly_historic_demand_pk PRIMARY KEY (timestamp)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS cammesa_db.hourly_historic_demand
    OWNER to postgres;
