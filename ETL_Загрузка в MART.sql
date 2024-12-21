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