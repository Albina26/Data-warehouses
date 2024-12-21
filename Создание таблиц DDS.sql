-- (23). Создание таблиц в dds
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