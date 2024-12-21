-- Создание таблиц и загрузка данных в ODS

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
  cancelled int NOT NULL,
  cancellation_code char(1),
  air_time float,
  distance float,
  weather_delay float,
  processed_dttm timestamp NOT NULL DEFAULT now()
);

CREATE TABLE kdz11_ods.weather (
	icao_code varchar(10) NOT NULL DEFAULT 'KDTW',
	local_datetime varchar(25) NOT NULL,
	t_air_temperature numeric(3, 1) NOT NULL,
	p0_sea_lvl numeric(4, 1) NOT NULL,
	p_station_lvl numeric(4, 1) NOT NULL,
	u_humidity int4 NOT NULL,
	dd_wind_direction varchar(100) NULL,
	ff_wind_speed int4 NULL,
	ff10_max_gust_value int4 NULL,
	ww_present varchar(100) NULL,
	ww_recent varchar(50) NULL,
	c_total_clouds varchar(200) NOT NULL,
	vv_horizontal_visibility numeric(3, 1) NOT NULL,
	td_temperature_dewpoint numeric(3, 1) NOT NULL,
	processed_dttm timestamp NOT NULL DEFAULT now()
);

CREATE TABLE kdz11_ods.airports (
    id int PRIMARY KEY,
    ident varchar(10) NOT NULL,
    type varchar(100) NOT NULL,
    name varchar(100) NOT NULL,
    latitude_deg numeric(10, 7),
    longitude_deg numeric(11, 7),
    elevation_ft int,
    continent varchar(100),
    iso_country varchar(100) NOT NULL,
    iso_region varchar(100),
    municipality varchar(100),
    scheduled_service BOOLEAN,
    gps_code varchar(100),
    iata_code varchar(100) NOT NULL,
    local_code varchar(100),
    home_link text,
    wikipedia_link text,
    keywords text,
    processed_dttm timestamp NOT NULL DEFAULT now()
);