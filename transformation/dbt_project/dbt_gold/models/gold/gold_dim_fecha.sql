{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    CAST(DATE_FORMAT(event_datetime, 'yyyyMMdd') AS INT) AS fecha_key,
    CAST(event_datetime AS DATE)                          AS fecha,
    review_year                                           AS anio,
    review_month                                          AS mes,
    review_day                                            AS dia,
    day_of_week                                           AS dia_semana,
    QUARTER(event_datetime)                               AS trimestre,
    CASE WHEN day_of_week IN (1, 7) THEN true ELSE false END AS es_fin_de_semana
FROM {{ ref('silver_reviews') }}
WHERE event_datetime IS NOT NULL