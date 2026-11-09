-- Q1 (SQL primary): mean and sample stddev of US population, 2013-2018
CREATE MATERIALIZED VIEW rearc.gold.population_stats AS
SELECT AVG(population)         AS mean_population,
       STDDEV_SAMP(population) AS stddev_population
FROM rearc.silver.population
WHERE year BETWEEN 2013 AND 2018;

-- Q2 (SQL primary): best year per series.
-- Q05 excluded (annual average). Tie-break: most recent year.
CREATE MATERIALIZED VIEW rearc.gold.best_year_per_series AS
WITH flagged AS (
  SELECT *,
         MAX(CASE WHEN period <> 'Q05' THEN 1 ELSE 0 END)
           OVER (PARTITION BY series_id) AS has_quarters
  FROM rearc.silver.bls_data
),
yearly AS (
  SELECT series_id, year,
         SUM(value) AS total_value,
         CASE WHEN has_quarters = 1 THEN 'sum_of_quarters'
              ELSE 'annual_only_Q05' END AS basis
  FROM flagged
  WHERE (has_quarters = 1 AND period <> 'Q05')
     OR (has_quarters = 0 AND period = 'Q05')
  GROUP BY series_id, year, has_quarters
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (
           PARTITION BY series_id
           ORDER BY total_value DESC, year DESC) AS rn
  FROM yearly
)
SELECT r.series_id, s.series_label, r.year AS best_year,
       r.total_value, r.basis
FROM ranked r
LEFT JOIN rearc.silver.series s USING (series_id)
WHERE r.rn = 1;