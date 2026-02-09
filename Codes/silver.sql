/* ===============================================
   Useing  DB and Schema
   =============================================== */
   
USE DATABASE energy_db;

/* ===============================================
   Enriching Source data
   =============================================== */
   
CREATE OR REPLACE TABLE energy_db.bronze.enriched_src_stream 
AS
SELECT
    id,
    name,
    latitude,
    longitude,
    altitude_m,
    land_area_m2,
    country,
    subregion,
    status,
    production_records_date,
    capacity_MW,
    panel_type,
    panel_count,
    production_records:"January"   AS January,
    production_records:"February"  AS February,
    production_records:"March"     AS March,
    production_records:"April"     AS April,
    production_records:"May"       AS May,
    production_records:"June"      AS June,
    production_records:"July"      AS July,
    production_records:"August"    AS August,
    production_records:"September" AS September,
    production_records:"October"   AS October,
    production_records:"November"  AS November,
    production_records:"December"  AS December
FROM energy_db.bronze.source;

/* ===============================================
   Defining Source For Dimention Table
   =============================================== */
   
CREATE OR REPLACE TABLE energy_db.bronze.Src_DimLocation(
    id STRING,
    name STRING,
    latitude FLOAT,
    longitude FLOAT,
    altitude_m INTEGER,
    land_area_m2 INTEGER,
    country STRING,
    subregion STRING,
    panel_type STRING,
    processDate DATETIME
);

CREATE OR REPLACE TABLE energy_db.bronze.Src_DimProduction(
    id STRING,
    status STRING,
    production_records_date NUMBER(4),
    January INTEGER,
    February INTEGER,
    March INTEGER,
    April INTEGER,
    May INTEGER,
    June INTEGER,
    July INTEGER,
    August INTEGER,
    September INTEGER,
    October INTEGER,
    November INTEGER,
    December INTEGER,
    processDate DATETIME
);

/* ===============================================
   Merge Type 1 into Source of Dimention 
   =============================================== */
   
MERGE INTO energy_db.bronze.Src_DimLocation t
USING energy_db.bronze.enriched_src_stream s
ON s.id = t.id
WHEN MATCHED THEN UPDATE SET 
t.id = s.id,
t.name = s.name,
t.latitude = s.latitude,
t.longitude = s.longitude,
t.altitude_m = s.altitude_m,
t.land_area_m2 = s.land_area_m2,
t.country = s.country,
t.subregion = s.subregion,
t.panel_type = s.panel_type,
t.processDate = current_timestamp()
WHEN NOT MATCHED THEN INSERT 
(id,name,latitude,longitude,altitude_m,land_area_m2,country,subregion,panel_type,processDate)
values
(s.id,s.name,s.latitude,s.longitude,s.altitude_m,s.land_area_m2,s.country,s.subregion,s.panel_type,current_timestamp());

MERGE INTO energy_db.bronze.Src_DimProduction t
USING energy_db.bronze.enriched_src_stream s
ON (s.id = t.id AND s.production_records_date = t.production_records_date AND s.status = t.status)
WHEN MATCHED THEN UPDATE SET 
t.id = s.id, 
t.status = s.status,
t.production_records_date = s.production_records_date,
t.January = s.January,
t.February = s.February,
t.March = s.March,
t.April = s.April,
t.May = s.May,
t.June = s.June,
t.July = s.July,
t.August = s.August,
t.September = s.September,
t.October = s.October,
t.November = s.November,
t.December = s.December,
t.processDate = current_timestamp()
WHEN NOT MATCHED THEN INSERT 
(id,status,production_records_date,January,February,March,April,May,June,July,August,September,October,November,December,processDate)
values
(s.id,s.status,s.production_records_date,s.January,s.February,s.March,s.April,s.May,s.June,s.July,s.August,s.September,s.October,s.November,s.December,current_timestamp());
