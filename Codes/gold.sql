/* ===============================================
   Using DB For a new Worksheet
   =============================================== */
   
USE DATABASE energy_db;

/* ===============================================
   Defining Dimention and Fact Tables
   =============================================== */

CREATE OR REPLACE TABLE energy_db.gold.DimLocation(
    DimLocationKey INTEGER,
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

CREATE OR REPLACE TABLE energy_db.gold.DimProduction(
    DimProductionKey INTEGER,
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

CREATE OR REPLACE TABLE energy_db.gold.Fact(
    DimLocationKey INTEGER,
    DimProductionKey INTEGER,
    capacity_MW FLOAT,
    panel_count INTEGER,
    processDate DATETIME
);

/* ===============================================
   Defining Function to Find MAX of Last Load and 
   MAX of Surrogate Key on Dimention Tables
   =============================================== */

CREATE OR REPLACE FUNCTION energy_db.gold.get_last_load_date_DimLocation()
RETURNS DATETIME
LANGUAGE SQL
AS
$$
SELECT COALESCE(MAX(processDate),'1000-08-06 00:00:00.000') FROM energy_db.gold.DimLocation
$$;

CREATE OR REPLACE FUNCTION energy_db.gold.get_last_surrogateKey_DimLocation()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
SELECT COALESCE(count(*),0) FROM energy_db.gold.DimLocation
$$;

CREATE OR REPLACE FUNCTION energy_db.gold.get_last_load_date_DimProduction()
RETURNS DATETIME
LANGUAGE SQL
AS
$$
SELECT COALESCE(MAX(processDate),'1000-08-06 00:00:00.000') FROM energy_db.gold.DimProduction
$$;

CREATE OR REPLACE FUNCTION energy_db.gold.get_last_surrogateKey_DimProduction()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
SELECT COALESCE(count(*),0) FROM energy_db.gold.DimProduction
$$;

/* ===============================================
   Creating Surrogate key For Location Dimention and 
   Merge Type 1 From Src_DimLocation to Dimention 
   Table "DimLocation"
   =============================================== */

CREATE OR REPLACE VIEW energy_db.gold.src 
AS
SELECT * FROM energy_db.bronze.Src_DimLocation
WHERE processDate > get_last_load_date_DimLocation();

CREATE OR REPLACE VIEW energy_db.gold.temp
AS 
SELECT 
    t.DimLocationKey,
    s.*
FROM energy_db.gold.src s
LEFT JOIN energy_db.gold.DimLocation t
ON s.id = t.id;

CREATE OR  REPLACE VIEW energy_db.gold.oldtemp
AS
SELECT *
FROM energy_db.gold.temp
WHERE DimLocationKey is not null;

CREATE OR  REPLACE TABLE energy_db.gold.newtemp
AS
SELECT *
FROM energy_db.gold.temp
WHERE DimLocationKey is null;

ALTER TABLE energy_db.gold.newtemp
DROP COLUMN DimLocationKey;

CREATE OR  REPLACE VIEW energy_db.gold.newtemp_enr
AS
SELECT
ROW_NUMBER() OVER (ORDER BY e.id)+get_last_surrogateKey_DimLocation() as DimLocationKey,
e.*
FROM energy_db.gold.newtemp e;

CREATE OR  REPLACE VIEW energy_db.gold.mainsrc
AS
SELECT *
FROM energy_db.gold.oldtemp
UNION ALL
SELECT *
FROM energy_db.gold.newtemp_enr;

MERGE INTO energy_db.gold.DimLocation t
USING energy_db.gold.mainsrc s
ON s.id = t.id
WHEN MATCHED THEN UPDATE SET 
t.DimLocationKey = s.DimLocationKey,
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
(DimLocationKey,id,name,latitude,longitude,altitude_m,land_area_m2,country,subregion,panel_type,processDate)
values
(s.DimLocationKey,s.id,s.name,s.latitude,s.longitude,s.altitude_m,s.land_area_m2,s.country,s.subregion,s.panel_type,current_timestamp());

/* ===============================================
   Creating Surrogate key For production Dimention and 
   Merge Type 1 From Src_DimProduction to Dimention 
   Table "DimProduction"
   =============================================== */

CREATE OR REPLACE VIEW energy_db.gold.src 
AS
SELECT * FROM energy_db.bronze.Src_DimProduction
WHERE processDate > get_last_load_date_DimProduction();


CREATE OR REPLACE VIEW energy_db.gold.temp
AS 
SELECT 
    t.DimProductionKey,
    s.*
FROM energy_db.gold.src s
LEFT JOIN energy_db.gold.DimProduction t
ON s.id = t.id AND s.production_records_date = t.production_records_date AND s.status = t.status;


CREATE OR  REPLACE VIEW energy_db.gold.oldtemp
AS
SELECT *
FROM energy_db.gold.temp
WHERE DimProductionKey is not null;


CREATE OR  REPLACE TABLE energy_db.gold.newtemp
AS
SELECT *
FROM energy_db.gold.temp
WHERE DimProductionKey is null;


ALTER TABLE energy_db.gold.newtemp
DROP COLUMN DimProductionKey;


CREATE OR  REPLACE VIEW energy_db.gold.newtemp_enr
AS
SELECT
ROW_NUMBER() OVER (ORDER BY e.id)+get_last_surrogateKey_DimProduction() as DimProductionKey,
e.*
FROM energy_db.gold.newtemp e;


CREATE OR  REPLACE VIEW energy_db.gold.mainsrc
AS
SELECT *
FROM energy_db.gold.oldtemp
UNION ALL
SELECT *
FROM energy_db.gold.newtemp_enr;

MERGE INTO energy_db.gold.DimProduction t
USING energy_db.gold.mainsrc s
ON (s.id = t.id AND s.production_records_date = t.production_records_date AND s.status = t.status)
WHEN MATCHED THEN UPDATE SET 
t.DimProductionKey = s.DimProductionKey,
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
(DimProductionKey,id,status,production_records_date,January,February,March,April,May,June,July,August,September,October,November,December,processDate)
values
(s.DimProductionKey,s.id,s.status,s.production_records_date,s.January,s.February,s.March,s.April,s.May,s.June,s.July,s.August,s.September,s.October,s.November,s.December,current_timestamp());

/* ===============================================
   Creating Fact Table
   =============================================== */

CREATE OR REPLACE TABLE energy_db.gold.src_Fact 
AS
SELECT DISTINCT
    L.DimLocationKey,
    P.DimProductionKey,
    S.capacity_MW,
    S.panel_count,
    CURRENT_TIMESTAMP AS processDate
FROM energy_db.bronze.enriched_src_stream S
LEFT JOIN energy_db.gold.DimLocation L
    ON S.id = L.id
LEFT JOIN energy_db.gold.DimProduction P
    ON S.id = P.id
    AND S.production_records_date = P.production_records_date
    AND S.status = P.status;

    
MERGE INTO energy_db.gold.Fact t
USING energy_db.gold.src_Fact s
ON s.DimLocationKey = t.DimLocationKey AND s.DimProductionKey = t.DimProductionKey AND s.capacity_MW = t.capacity_MW AND s.panel_count = t.panel_count
WHEN MATCHED THEN UPDATE SET 
t.DimLocationKey = s.DimLocationKey,
t.DimProductionKey = s.DimProductionKey,
t.capacity_MW = s.capacity_MW,
t.panel_count = s.panel_count,
t.processDate = current_timestamp()
WHEN NOT MATCHED THEN INSERT 
(DimLocationKey,DimProductionKey,capacity_MW,panel_count,processDate)
values
(s.DimLocationKey,s.DimProductionKey,s.capacity_MW,s.panel_count,current_timestamp());
    
/* ===============================================
   Output
   =============================================== */
SELECT * FROM energy_db.gold.DimProduction;

SELECT * fROM energy_db.gold.DimLocation;

SELECT * FROM energy_db.gold.Fact;


   