CREATE MATERIALIZED VIEW rearc.gold.prs30006032_q01_with_population AS
SELECT b.series_id, b.year, b.period, b.value, p.population
FROM rearc.silver.bls_data b
LEFT JOIN rearc.silver.population p ON b.year = p.year
WHERE b.series_id = 'PRS30006032' AND b.period = 'Q01'
ORDER BY b.year;