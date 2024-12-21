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