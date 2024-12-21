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