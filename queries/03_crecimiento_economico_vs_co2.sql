-- Pregunta 3: ¿Qué países logran crecimiento económico con menor impacto ambiental?
-- Busca países con crecimiento de GDP positivo y baja intensidad de CO₂
-- para identificar casos de desacople entre economía y emisiones.

SELECT
    country,
    country_code,
    AVG(gdp_growth_pct)             AS promedio_crecimiento_gdp,
    AVG(co2_intensity_gdp)          AS promedio_intensidad_co2,
    AVG(co2_per_capita)             AS promedio_co2_per_capita,
    AVG(gdp_per_capita)             AS promedio_gdp_per_capita
FROM latam_sustainable_tourism.fact_tourism_emissions
WHERE gdp_growth_pct IS NOT NULL
  AND co2_intensity_gdp IS NOT NULL
  AND year >= 2000
GROUP BY country, country_code
ORDER BY promedio_crecimiento_gdp DESC, promedio_intensidad_co2 ASC