-- Первоначальная загрузка weather с помощью ETL

-- (4). Создаем и обновляем инкремент для weather после запуска STG
drop table if exists kdz11_etl.increment_weather_ods_stg;
create table kdz11_etl.increment_weather_ods_stg (processed_dttm timestamp);
insert into kdz11_etl.increment_weather_ods_stg(processed_dttm) values ('24-12-16 18:10:10');

-- Последующая загрузка - Создаем и обновляем инкремент для weather после запуска STG
--drop table if exists kdz11_etl.increment_weather_ods_stg;
--create table kdz11_etl.increment_weather_ods_stg as 
--select max(processed_dttm) as processed_dttm
--from kdz11_ods.weather;

-- (5). Создаем таблицу в ETL, которая добавляет данные weather после инкремента
drop table if exists kdz11_etl.weather_add;
create table kdz11_etl.weather_add as 
    select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity ::integer, dd_wind_direction, ff_wind_speed,
        ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint, now()
    from kdz11_ods.weather ow, kdz11_etl.increment_weather_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- (6). Загружаем все данные о weather в STG
insert into kdz11_stg.weather
select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed,
    ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint, now()
from kdz11_etl.weather_add;



-- Первоначальная загрузка flights с помощью ETL

-- (7). Создаем и обновляем инкремент для flights после запуска STG
drop table if exists kdz11_etl.increment_flights_ods_stg;
create table kdz11_etl.increment_flights_ods_stg (processed_dttm timestamp);
insert into kdz11_etl.increment_flights_ods_stg(processed_dttm) values ('24-12-16 18:10:10');

-- (8). Создаем таблицу в ETL, которая добавляет данные flights после инкремента
drop table if exists kdz11_etl.flights_add;
create table kdz11_etl.flights_add as 
    select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_new as dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    op_unique_carrier as reporting_airline, tail_number, op_carrier_fl_num as flight_number, distance, origin, dest, now() 
    from kdz11_ods.flight ow, kdz11_etl.increment_flights_ods_stg i
    where ow.processed_dttm > i.processed_dttm;

-- (9). Загружаем все данные о flights в STG
insert into kdz11_stg.flights
select year, quarter, month, flight_date, dep_time, crs_dep_time, air_time, dep_delay_minutes, cancelled, cancellation_code, weather_delay,
    reporting_airline, tail_number, flight_number, distance, origin, dest, now()
from kdz11_etl.flights_add;