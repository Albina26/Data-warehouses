-- Общий (итоговый) скрипт

-- 1. Создание таблиц в ODS
drop table if exists kdz11_ods.flight;

CREATE TABLE kdz11_ods.flight (
  year int NOT NULL,
  quarter int NOT NULL,
  month int NOT NULL,
  flight_date date NOT NULL,
  op_unique_carrier varchar(100),
  tail_number varchar(10),
  op_carrier_fl_num int,
  origin_airport_id int,
  origin_airport_seq_id int,
  origin_city_market_id int,
  origin varchar(10),
  dest_airport_id int,
  dest_airport_seq_id int,
  dest_city_market_id int,
  dest varchar(10),
  crs_dep_time time NOT NULL,
  dep_time time,
  dep_delay_new float,
  cancelled float default 0,
  cancellation_code char(1),
  air_time float,
  distance float,
  weather_delay float,
  processed_dttm timestamp NOT NULL DEFAULT now()
);

drop table if exists kdz11_ods.weather;
CREATE TABLE kdz11_ods.weather (
    	icao_code varchar(10) NOT NULL DEFAULT 'KDTW',
        local_datetime varchar(25) NOT NULL,
        t_air_temperature numeric(3, 1),
        p0_sea_lvl numeric(4, 1),
        p_station_lvl numeric(4, 1),
        u_humidity int,
        dd_wind_direction varchar(100),
        ff_wind_speed int,
        ff10_max_gust_value int,
        ww_present varchar(100),
        ww_recent varchar(50),
        c_total_clouds varchar(200),
        vv_horizontal_visibility numeric(3, 1),
        td_temperature_dewpoint numeric(3, 1),
        processed_dttm timestamp DEFAULT now()
);

drop table if exists kdz11_ods.airports;
CREATE TABLE kdz11_ods.airports (
    id varchar PRIMARY KEY,
    ident varchar(10) NOT NULL,
    type varchar(100) NOT NULL,
    name varchar(100) NOT NULL,
    latitude_deg varchar(100),
    longitude_deg varchar(100),
    elevation_ft varchar,
    continent varchar(100),
    iso_country varchar(100),
    iso_region varchar(100),
    municipality varchar(100),
    scheduled_service BOOLEAN,
    gps_code varchar(100),
    iata_code varchar(100),
    local_code varchar(100),
    home_link text,
    wikipedia_link text,
    keywords text,
    processed_dttm timestamp NOT NULL DEFAULT now()
);



-- 2. Загрузка данных weather, Michigan 052022, airports через Power Shell
\copy kdz11_ods.weather (local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed, ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/KDTW.Weather.csv' with delimiter ';' CSV HEADER;
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 052022.csv' with delimiter ',' CSV HEADER;
\copy kdz11_ods.airports (id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/airports.csv' with delimiter ';' CSV HEADER;



-- 3. Создание таблиц и загрузка данных в STG
drop table if exists kdz11_stg.flights;
CREATE TABLE kdz11_stg.flights (
    	year int NOT NULL,
        quarter int NOT NULL,
        month int NOT NULL,
        flight_date date NOT NULL,
        dep_time time,
        crs_dep_time time NOT NULL,
        air_time float,
        dep_delay_minutes float,
        cancelled int NOT NULL,
        cancellation_code char(1),
        weather_delay float,
        reporting_airline varchar(10),
        tail_number varchar(10),
        flight_number varchar(15) NOT NULL,
        distance float,
        origin varchar(10),
        dest varchar(10),
        processed_dttm timestamp NOT NULL DEFAULT now(),
        CONSTRAINT flights_pkey PRIMARY KEY (flight_date, flight_number, origin, dest, crs_dep_time)
);

drop table if exists kdz11_stg.weather;
CREATE TABLE kdz11_stg.weather (
    	icao_code varchar(10) NOT NULL,
        local_datetime varchar(25) NOT NULL,
        t_air_temperature numeric(3, 1),
        p0_sea_lvl numeric(4, 1),
        p_station_lvl numeric(4, 1),
        u_humidity int,
        dd_wind_direction varchar(100),
        ff_wind_speed int,
        ff10_max_gust_value varchar(10),
        ww_present varchar(100),
        ww_recent varchar(50),
        c_total_clouds varchar(200),
        vv_horizontal_visibility numeric(3, 1),
        td_temperature_dewpoint numeric(3, 1),
        processed_dttm timestamp NOT NULL DEFAULT now(),
        PRIMARY KEY (icao_code, local_datetime)
); 



-- Первоначальная загрузка weather с помощью ETL
-- 4. Создаем и обновляем инкремент для weather после запуска STG
drop table if exists kdz11_etl.increment_weather_ods_stg;
create table kdz11_etl.increment_weather_ods_stg (processed_dttm timestamp);
insert into kdz11_etl.increment_weather_ods_stg(processed_dttm) values ('24-12-16 18:10:10');

-- Последующая загрузка - Создаем и обновляем инкремент для weather после запуска STG
--drop table if exists kdz11_etl.increment_weather_ods_stg;
--create table kdz11_etl.increment_weather_ods_stg as 
--select max(processed_dttm) as processed_dttm
--from kdz11_ods.weather;

-- 5. Создаем таблицу в ETL, которая добавляет данные weather после инкремента
drop table if exists kdz11_etl.weather_add;
create table kdz11_etl.weather_add as 
    select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity ::integer, dd_wind_direction, ff_wind_speed,
        ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint, now()
    from kdz11_ods.weather ow, kdz11_etl.increment_weather_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- 6. Загружаем все данные о weather в STG
insert into kdz11_stg.weather
select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed,
    ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint, now()
from kdz11_etl.weather_add;



-- Первоначальная загрузка flights с помощью ETL
-- 7. Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg (processed_dttm timestamp);
insert into kdz11_etl.increment_flights_ods_stg(processed_dttm) values ('24-12-16 18:10:10');

-- 8. Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- 9. Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;



--Последующая загрузка flights с помощью ETL
-- 10. Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- 11. Загрузка данных Michigan 062022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 062022.csv' with delimiter ',' CSV HEADER;

-- 12. Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- 13. Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;


-- 14. Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- 15. Загрузка данных Michigan 072022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 072022.csv' with delimiter ',' CSV HEADER;

-- 16. Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- 17. Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;


-- 18. Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- 19. Загрузка данных Michigan 082022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 082022.csv' with delimiter ',' CSV HEADER;

-- 20. Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.weather;

-- 21. Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- 22. Загружаем все данные о flights в STG
INSERT INTO kdz11_stg.flights
    SELECT year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
        reporting_airline, tail_number, flight_number, distance, origin, dest, now()
    FROM kdz11_etl.flights_add
    ON CONFLICT (flight_date, flight_number, origin, dest, crs_dep_time) 
    DO UPDATE SET 
        dep_time = EXCLUDED.dep_time,
        air_time = EXCLUDED.air_time,
        dep_delay_minutes = EXCLUDED.dep_delay_minutes,
        cancelled = EXCLUDED.cancelled,
        cancellation_code = EXCLUDED.cancellation_code,
        weather_delay = EXCLUDED.weather_delay,
        reporting_airline = EXCLUDED.reporting_airline,
        tail_number = EXCLUDED.tail_number,
        distance = EXCLUDED.distance;



-- 23. Создание таблиц в DDS
drop table if exists kdz11_dds.airport_weather;
CREATE TABLE kdz11_dds.airport_weather (
    	airport_dk int NOT NULL, -- постоянный ключ аэропорта. нужно взять из таблицы аэропортов
        weather_type_dk char(6) NOT NULL, -- постоянный ключ типа погоды. заполняется по формуле
        cold smallint default(0),        
        rain smallint default(0),
        thunderstorm smallint default(0),
        drizzle smallint default(0),
        fog_mist smallint default(0),
        high_wind smallint default(0),
        t int NULL,
        max_gws int NULL,
        w_speed int NULL,
        date_start timestamp NOT NULL,
        date_end timestamp NOT NULL default('3000-01-01'::timestamp),
        processed_dttm timestamp default(now()),
        PRIMARY KEY (airport_dk, date_start)
);

drop table if exists kdz11_dds.flights;
CREATE TABLE kdz11_dds.flights (
    	year int NULL,
        quarter int NULL,
        month int NULL,
        flight_scheduled_date date NULL,
        flight_actual_date date NULL,
        flight_dep_scheduled_ts timestamp NOT NULL,
        flight_dep_actual_ts timestamp NULL,
        report_airline varchar(10) NOT NULL,
        tail_number varchar(10) NOT NULL,
        flight_number_reporting_airline varchar(15) NOT NULL,
        airport_origin_dk int NULL, --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
        origin_code varchar(5) null,
        airport_dest_dk int NULL,  --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
        dest_code varchar(5) null,
        dep_delay_minutes float NULL,
        cancelled int NOT NULL,
        cancellation_code char(1) NULL,
        weather_delay float NULL,
        air_time float NULL,
        distance float NULL,
        processed_dttm timestamp default(now()),
        CONSTRAINT lights_pk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);

drop table if exists kdz11_dds.weather_type;
CREATE TABLE kdz11_dds.weather_type (
    	weather_type_rk int primary key,
    	cold smallint default(0),
        rain smallint default(0),
        thunderstorm smallint default(0),
        fog_mist smallint default(0),
        drizzle smallint default(0),
        high_wind smallint default(0)
);

drop table if exists kdz11_dds.airports;
create table kdz11_dds.airports(
    id varchar PRIMARY KEY,
    ident varchar(10) NOT NULL,
    type varchar(100) NOT NULL,
    name varchar(100) NOT NULL,
    latitude_deg varchar(100),
    longitude_deg varchar(100),
    elevation_ft varchar,
    continent varchar(100),
    iso_country varchar(100),
    iso_region varchar(100),
    municipality varchar(100),
    scheduled_service BOOLEAN,
    gps_code varchar(100),
    iata_code varchar(100),
    local_code varchar(100),
    home_link text,
    wikipedia_link text,
    keywords text,
    processed_dttm timestamp NOT NULL DEFAULT now()
);



-- (24). Загрузка данных в kdz11_dds.weather_type

\copy kdz11_dds.weather_type (weather_type_rk, cold, rain, thunderstorm, fog_mist, drizzle, high_wind) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/weather_type.csv' with delimiter ',' CSV HEADER;



-- Загрузка данных в kdz11_dds.airport_weather
-- (25).Создаем таблицу в ETL, которая добавляет только последние актуальные данные из STG
drop table if exists kdz11_etl.weather_actual_for_dds;
create table kdz11_etl.weather_actual_for_dds as 
select distinct on (icao_code, local_datetime)
    icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity ::integer, dd_wind_direction, ff_wind_speed,
    ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint
from (
    select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity ::integer, dd_wind_direction, ff_wind_speed,
  ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint
    from kdz11_stg.weather
    order by icao_code, local_datetime, processed_dttm desc
);

-- (26). Создаем таблицу для обработки данных о погодных условиях в kdz11_etl.weather_actual_for_dds для таблицы dds
drop table if exists kdz11_etl.weather_process;
create table kdz11_etl.weather_process as 
(
    SELECT 
 		a.id AS airport_dk,
     	w.icao_code,
        to_timestamp(w.local_datetime, 'DD.MM.YYYY HH24:MI:SS') AS local_datetime,  -- Измененный формат
        w.t_air_temperature AS t,
        w.ff_wind_speed AS w_speed,
        w.ff10_max_gust_value AS max_gws,
        CASE WHEN w.ww_present ILIKE '%rain%' THEN 1 ELSE 0 END AS rain,
        CASE WHEN w.ww_present ILIKE '%thunderstorm%' THEN 1 ELSE 0 END AS thunderstorm,
        CASE WHEN w.ww_present ILIKE '%fog%' OR w.ww_present ILIKE '%mist%' OR w.ww_present ILIKE '%haze%' THEN 1 ELSE 0 END AS fog_mist,
        CASE WHEN w.ww_present ILIKE '%drizzle%' THEN 1 ELSE 0 END AS drizzle,
        CASE WHEN w.ff_wind_speed > 6 THEN 1 ELSE 0 END AS high_wind,
        CASE WHEN w.t_air_temperature < 15 THEN 1 ELSE 0 END AS cold
        FROM kdz11_etl.weather_actual_for_dds w
    JOIN kdz11_ods.airports a ON a.ident = w.icao_code
);

-- (27). Создаем таблицу для обработки данных о погодных условиях в kdz11_etl.weather_actual_for_dds для таблицы dds
drop table if exists kdz11_etl.weather_process_date;
create table kdz11_etl.weather_process_date as 
SELECT 
    airport_dk,
    local_datetime AS date_start,
    LEAD(local_datetime) OVER (PARTITION BY airport_dk ORDER BY local_datetime) AS date_end,
    cold, rain, thunderstorm, fog_mist, drizzle, high_wind, t, max_gws, w_speed,
    CONCAT(cold, rain, thunderstorm, drizzle, fog_mist ) AS weather_type_dk
FROM kdz11_etl.weather_process;

-- (28). Вставим данные в DDS
insert into kdz11_dds.airport_weather
select airport_dk :: integer, weather_type_dk, cold, rain, thunderstorm, fog_mist, drizzle, high_wind, 
    t, max_gws :: integer, w_speed, date_start, COALESCE(date_end, date_start) AS date_end, now()
from kdz11_etl.weather_process_date;



-- Загрузка данных в kdz11_dds.flights
-- (29).Создаем таблицу в ETL, которая добавляет только последние актуальные данные из STG
drop table if exists kdz11_etl.flights_actual_for_dds;
create table kdz11_etl.flights_actual_for_dds as 
select distinct on (flight_date, flight_number, origin, dest, crs_dep_time)
    year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest
from (
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
  reporting_airline, tail_number, flight_number, distance, origin, dest
    from kdz11_stg.flights
    order by flight_date, flight_number, origin, dest, crs_dep_time, processed_dttm desc
);

-- (30). Создаем таблицу для обработки данных в kdz11_etl.flights_actual_for_dds для таблицы dds
drop table if exists kdz11_etl.flights_process;
CREATE TABLE kdz11_etl.flights_process AS 
(
    SELECT
 	 year, 
     quarter, 
     month,
     flight_date::DATE AS flight_scheduled_date,
     CASE
     WHEN cancelled = 0 THEN (flight_date || ' ' || crs_dep_time)::TIMESTAMP + (dep_delay_minutes || ' minutes')::INTERVAL
         ELSE NULL
         END AS flight_actual_date,
     (flight_date || ' ' || crs_dep_time)::TIMESTAMP AS flight_dep_scheduled_ts,
     CASE
     WHEN cancelled = 0 THEN (flight_date || ' ' || crs_dep_time)::TIMESTAMP + (dep_delay_minutes || ' minutes')::INTERVAL
         ELSE NULL
         END AS flight_dep_actual_ts,
     reporting_airline, 
     tail_number, 
     flight_number AS flight_number_reporting_airline,
     oa.id AS airport_origin_dk,
     origin AS origin_code,
     da.id AS airport_dest_dk,
     dest AS dest_code,
     dep_delay_minutes, 
     cancelled, 
     cancellation_code, 
     weather_delay, 
     air_time, 
     distance
     FROM
 kdz11_stg.flights
     JOIN
 kdz11_ods.airports oa ON origin = oa.iata_code
     JOIN
 kdz11_ods.airports da ON dest = da.iata_code
);

-- (31). Вставим данные в kdz11_dds.flights
insert into kdz11_dds.flights
select
    year, quarter, month, flight_scheduled_date, flight_actual_date,
    flight_dep_scheduled_ts, flight_dep_actual_ts, reporting_airline, COALESCE(tail_number, '001') AS tail_number, flight_number_reporting_airline,
    airport_origin_dk :: integer,origin_code,airport_dest_dk :: integer,dest_code,dep_delay_minutes,cancelled,cancellation_code,
        weather_delay,air_time,distance,now()
from kdz11_etl.flights_process;



-- (32). Загрузка данных в kdz11_dds.airports
insert into kdz11_dds.airports
select *
from kdz11_ods.airports;



-- (33). Создание таблицы в MART
drop table if exists kdz11_mart.fact_departure;
CREATE TABLE kdz11_mart.fact_departure (
    airport_origin_dk int NOT NULL,
                airport_destination_dk int, 
                flight_scheduled_ts date,
                flight_number varchar(10),
                weather_type_dk char(6) NOT NULL,
                flight_actual_time timestamp,
                distance float NULL,
                tail_number varchar(10) NOT NULL,
                airline varchar(10),
                dep_delay_min float NULL,
                cancelled int,
                cancellation_code char(1) NULL,
                t int NULL,
                max_gws int NULL,
                w_speed int NULL, 
                air_time float NULL,
                source_name varchar(10),
                processed_dttm timestamp default(now()),
                primary key (airport_origin_dk, airport_destination_dk, flight_scheduled_ts, flight_number)
);



-- (34). Создаем таблицу для обработки данных kdz11_etl.flights_with_weather
create TABLE kdz11_etl.flights_with_weather AS
SELECT
    f.airport_origin_dk,
    f.airport_dest_dk,
    f.flight_scheduled_date as flight_scheduled_ts,
    f.flight_number_reporting_airline as flight_number,
    aw.weather_type_dk,
    f.flight_dep_actual_ts as flight_actual_time,
    f.distance,
    f.tail_number,
    f.report_airline as airline,
    f.dep_delay_minutes as dep_delay_min,
    f.cancelled,
    f.cancellation_code,
    aw.t,
    aw.max_gws,
    aw.w_speed,
    f.air_time,
    'flights' as source_name  -- Указываем источник данных
FROM
    kdz11_dds.flights f
JOIN
    kdz11_dds.airport_weather aw ON f.airport_origin_dk = aw.airport_dk
    AND f.flight_scheduled_date BETWEEN aw.date_start AND aw.date_end;



-- (35). Вставляем данные в целевую таблицу kdz11_mart.fact_departure
INSERT INTO kdz11_mart.fact_departure (
    airport_origin_dk,
    airport_destination_dk,
    flight_scheduled_ts,
    flight_number,
    weather_type_dk,
    flight_actual_time,
    distance,
    tail_number,
    airline,
    dep_delay_min,
    cancelled,
    cancellation_code,
    t,
    max_gws,
    w_speed,
    air_time,
    source_name,
    processed_dttm
)
SELECT
    airport_origin_dk,
    airport_dest_dk,
    flight_scheduled_ts,
    flight_number,
    weather_type_dk,
    flight_actual_time,
    distance,
    tail_number,
    airline,
    dep_delay_min,
    cancelled,
    cancellation_code,
    t,
    max_gws,
    w_speed,
    air_time,
    source_name,
    NOW() as processed_dttm
FROM
    kdz11_etl.flights_with_weather
ON CONFLICT (airport_origin_dk, airport_destination_dk, flight_scheduled_ts, flight_number) DO NOTHING;