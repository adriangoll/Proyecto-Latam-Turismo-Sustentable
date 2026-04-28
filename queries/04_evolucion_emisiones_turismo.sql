-- Pregunta 4: ¿Cómo evolucionan las emisiones en función del turismo a lo largo del tiempo?
-- Analiza año a año la relación entre crecimiento de llegadas de turistas
-- y crecimiento de emisiones de CO₂ para detectar tendencias temporales.

SELECT
    year,
    country,
    country_code,
    tourist_arrivals,
    arrivals_growth_pct             AS crecimiento_llegadas_pct,
    co2,
    co2_growth_pct                  AS crecimiento_co2_pct,
    co2_per_tourist
FROM latam_sustainable_tourism.fact_tourism_emissions
WHERE year >= 2000
  AND tourist_arrivals IS NOT NULL
  AND co2 IS NOT NULL
ORDER BY country ASC, year ASC