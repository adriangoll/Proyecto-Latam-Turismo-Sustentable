-- Pregunta 2: ¿Qué medios de transporte turístico tienen mayor impacto ambiental?
-- Compara el promedio de CO₂ por turista según el transporte dominante
-- para identificar qué medio genera mayor huella de carbono.

SELECT
    dominant_transport,
    COUNT(*)                        AS cantidad_registros,
    AVG(co2_per_tourist)            AS promedio_co2_por_turista,
    AVG(pct_air)                    AS promedio_pct_aereo,
    AVG(pct_sea)                    AS promedio_pct_maritimo,
    AVG(pct_land)                   AS promedio_pct_terrestre
FROM latam_sustainable_tourism.fact_tourism_emissions
WHERE dominant_transport IS NOT NULL
  AND co2_per_tourist IS NOT NULL
GROUP BY dominant_transport
ORDER BY promedio_co2_por_turista DESC