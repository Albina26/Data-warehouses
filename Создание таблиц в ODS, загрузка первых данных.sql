-- Проект (цифрами (1) отмечен порядок (шаг) выполнения действий во всех скриптах)

-- (1). Создание таблиц в ODS
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



-- (2). Загрузка данных weather, Michigan 052022, airports через Power Shell
\copy kdz11_ods.weather (local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed, ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/KDTW.Weather.csv' with delimiter ';' CSV HEADER;
\copy kdz11_ods.flight (year, quarter, month,flight_date,op_unique_carrier,tail_number,op_carrier_fl_num,origin_airport_id,origin_airport_seq_id,origin_city_market_id,origin,dest_airport_id,dest_airport_seq_id,dest_city_market_id,dest,crs_dep_time,dep_time,dep_delay_new,cancelled,cancellation_code,air_time,distance,weather_delay) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/Michigan 052022.csv' with delimiter ',' CSV HEADER;
\copy kdz11_ods.airports (id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords) from '/Users/alisaleusina/Documents/Учеба_ВШЭ/3_курс/ХД/КДЗ/airports.csv' with delimiter ';' CSV HEADER;
