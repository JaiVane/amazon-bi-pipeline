{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    CAST(DATE_FORMAT(event_datetime, 'yyyyMMdd') AS INT) AS fecha_key,
    CAST(event_datetime AS DATE) AS fecha,
    YEAR(event_datetime) AS anio,
    MONTH(event_datetime) AS mes,
    DAY(event_datetime) AS dia,
    DAYOFWEEK(event_datetime) AS dia_semana,
    QUARTER(event_datetime) AS trimestre,
    CASE WHEN DAYOFWEEK(event_datetime) IN (1,7) THEN true ELSE false END AS es_fin_de_semana
FROM {{ source('silver', 'reviews_clean') }}
WHERE event_datetime IS NOT NULL