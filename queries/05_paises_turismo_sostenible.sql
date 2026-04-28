-- Pregunta 5: ¿Qué países muestran tendencias hacia un turismo más sostenible?
-- Usa el campo sustainability_label calculado en la capa gold
-- para identificar qué países tienen mejor desempeño sostenible
-- y cómo evolucionó su etiqueta a lo largo del tiempo.

SELECT
    country,
    country_code,
    year,
    sustainability_label,
    tourist_arrivals,
    co2_per_tourist,
    arrivals_growth_pct             AS crecimiento_llegadas_pct,
    co2_growth_pct                  AS crecimiento_co2_pct
FROM latam_sustainable_tourism.fact_tourism_emissions
WHERE sustainability_label IS NOT NULL
  AND year >= 2000
ORDER BY sustainability_label ASC, country ASC, year ASC