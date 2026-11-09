from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="rearc.silver.bls_data")
@dp.expect_or_drop("keys_not_null", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL")
@dp.expect_or_drop("valid_period", "period RLIKE '^Q0[1-5]$'")
@dp.expect("year_in_range", "year BETWEEN 1900 AND 2100")
@dp.expect("value_present", "value IS NOT NULL")
def silver_bls_data():
    return (spark.read.table("rearc.bronze.bls_data")
            .select("series_id",
                    F.col("year").cast("int").alias("year"),
                    "period",
                    F.expr("try_cast(value AS DOUBLE)").alias("value"),
                    "footnote_codes")
            .dropDuplicates(["series_id", "year", "period"]))

@dp.materialized_view(name="rearc.silver.series")
@dp.expect_or_drop("series_id_not_null", "series_id IS NOT NULL")
@dp.expect("label_present", "series_label IS NOT NULL")
def silver_series():
    return (spark.read.table("rearc.bronze.bls_series")
            .select("series_id", "sector_code", "class_code", "measure_code",
                    "duration_code", "seasonal", "base_year",
                    F.col("series_title").alias("series_label"),
                    F.col("begin_year").cast("int").alias("begin_year"),
                    "begin_period",
                    F.col("end_year").cast("int").alias("end_year"),
                    "end_period"))

@dp.materialized_view(name="rearc.silver.population")
@dp.expect_or_fail("year_not_null", "year IS NOT NULL")
@dp.expect_or_drop("population_positive", "population > 0")
def silver_population():
    return (spark.read.table("rearc.bronze.population")
            .select(F.col("year").cast("int").alias("year"),
                    F.col("population").cast("long").alias("population"))
            .dropDuplicates(["year"]))