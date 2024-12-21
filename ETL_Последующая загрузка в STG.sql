--Последующая загрузка flights с помощью ETL

-- (10). Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- (11). Загрузка данных Michigan 062022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 062022.csv' with delimiter ',' CSV HEADER;

-- (12). Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- (13). Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;


-- (14). Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- (15). Загрузка данных Michigan 072022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 072022.csv' with delimiter ',' CSV HEADER;

-- (16). Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- (17). Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;


-- (18). Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.flight;

-- (19). Загрузка данных Michigan 082022 через Power Shell
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 082022.csv' with delimiter ',' CSV HEADER;

-- (20). Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg as 
select max(processed_dttm) as processed_dttm
from kdz11_ods.weather;

-- (21). Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- (22). Загружаем все данные о flights в STG
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