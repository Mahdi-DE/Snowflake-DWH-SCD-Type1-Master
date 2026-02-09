/* ===============================================
   Defining DB and Schema
   =============================================== */

CREATE DATABASE energy_db;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

/* ===============================================
   Defining Internal Stage
   =============================================== */
   
CREATE OR REPLACE STAGE energy_db.bronze.bronze_stage;
list @energy_db.bronze.bronze_stage;

/* ===============================================
   Defining File Format
   =============================================== */
   
CREATE OR REPLACE FILE FORMAT energy_db.bronze.json_format
type = 'json';

/* ===============================================
   Defining Source Table
   =============================================== */
   
CREATE OR REPLACE TABLE energy_db.bronze.source (
    id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    altitude_m INTEGER,
    land_area_m2 INTEGER,
    country STRING,
    subregion STRING,
    status STRING,
    capacity_MW FLOAT,
    panel_type STRING,
    panel_count INTEGER,
    production_records_date NUMBER(4),
    production_records VARIANT
);

/* ===============================================
   Inserting into Source From Internal Stage
   =============================================== */

INSERT INTO energy_db.bronze.source (
    id,
    name,
    latitude,
    longitude,
    altitude_m,
    land_area_m2,
    country,
    subregion,
    status,
    capacity_MW,
    panel_type,
    panel_count,
    production_records_date,
    production_records
)
SELECT
  t.$1:location.id,
  t.$1:location.name,
  t.$1:location.geo.latitude,
  t.$1:location.geo.longitude,
  t.$1:location.geo.altitude_m,
  t.$1:location.region.land_area_m2,
  t.$1:location.region.country,
  t.$1:location.region.subregion,
  t.$1:location.status_info.status,
  t.$1:technical_specs.capacity.value,
  t.$1:technical_specs.panels.details.type,
  t.$1:technical_specs.panels.details.count,
  t.$1:operations.production_records.yearly[0].year,
  t.$1:operations.production_records.yearly[0].monthly,
FROM @energy_db.bronze.bronze_stage (FILE_FORMAT => energy_db.bronze.json_format) t;





Drop stage energy_db.bronze.bronze_stage;