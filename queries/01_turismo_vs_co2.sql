-- Pregunta 1: ¿Existe relación entre el crecimiento del turismo
-- y el aumento de emisiones de CO₂ en LATAM?
-- Agrupa por año y suma llegadas de turistas y emisiones de CO₂
-- para ver la evolución conjunta en la región.

SELECT
    year,
    SUM(tourist_arrivals)   AS total_arrivals,
    SUM(co2)                AS total_co2
FROM latam_sustainable_tourism.fact_tourism_emissions
WHERE year >= 2000
GROUP BY year
ORDER BY year ASC