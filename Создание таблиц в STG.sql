-- (3). Создание таблиц в STG
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