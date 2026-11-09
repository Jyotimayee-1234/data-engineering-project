# Databricks notebook source
# Alternative (non-primary) implementations of the three Gold questions, plus validation that they
# match the primary pipeline outputs.
#
# Primary (in the pipeline): Spark SQL for Q1, Q2 and Q3.
# Alternative (here): PySpark for Q1 and Q2, and a standalone SQL query for Q3.
#
# Q1 is compared with a relative tolerance (floating-point aggregation order can differ between runs).
# Q2 and Q3 are compared exactly with exceptAll in both directions.
# Run in a Databricks notebook after the pipeline has completed.

# COMMAND ----------

from pyspark.sql import functions as F, Window

def q1_pyspark():
    return (spark.read.table("rearc.silver.population")
            .filter(F.col("year").between(2013, 2018))
            .agg(F.avg("population").alias("mean_population"),
                 F.stddev_samp("population").alias("stddev_population")))

def q2_pyspark():
    d = spark.read.table("rearc.silver.bls_data")
    d = d.withColumn("has_quarters",
                     F.max(F.when(F.col("period") != "Q05", 1).otherwise(0))
                      .over(Window.partitionBy("series_id")))
    yearly = (d.filter(((F.col("has_quarters") == 1) & (F.col("period") != "Q05")) |
                       ((F.col("has_quarters") == 0) & (F.col("period") == "Q05")))
                .groupBy("series_id", "year", "has_quarters")
                .agg(F.sum("value").alias("total_value"))
                .withColumn("basis", F.when(F.col("has_quarters") == 1, "sum_of_quarters")
                                      .otherwise("annual_only_Q05")))
    w = Window.partitionBy("series_id").orderBy(F.col("total_value").desc(), F.col("year").desc())
    best = yearly.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    s = spark.read.table("rearc.silver.series").select("series_id", "series_label")
    return (best.join(s, "series_id", "left")
                .select("series_id", "series_label",
                        F.col("year").alias("best_year"), "total_value", "basis"))

def q3_sql():
    return spark.sql("""
        SELECT b.series_id, b.year, b.period, b.value, p.population
        FROM rearc.silver.bls_data b
        LEFT JOIN rearc.silver.population p ON b.year = p.year
        WHERE b.series_id = 'PRS30006032' AND b.period = 'Q01'
        ORDER BY b.year""")

# COMMAND ----------

# Validation
def same(primary_table, alt_df, name):
    p = spark.read.table(primary_table)
    a = alt_df.select(*p.columns)
    missing = p.exceptAll(a).count()
    extra = a.exceptAll(p).count()
    assert missing == 0 and extra == 0, f"{name}: {missing} missing, {extra} extra"
    print(f"OK  {name}: identical ({p.count()} rows)")

def same_numeric(primary_table, alt_df, name, tol=1e-6):
    p = spark.read.table(primary_table).collect()[0]
    a = alt_df.collect()[0]
    for col in spark.read.table(primary_table).columns:
        pv, av = float(p[col]), float(a[col])
        assert abs(pv - av) <= tol * max(1.0, abs(pv)), f"{name}.{col}: {pv} vs {av}"
    print(f"OK  {name}: matches within tolerance {tol}")

same_numeric("rearc.gold.population_stats", q1_pyspark(), "Q1")
same("rearc.gold.best_year_per_series", q2_pyspark(), "Q2")
same("rearc.gold.prs30006032_q01_with_population", q3_sql(), "Q3")
