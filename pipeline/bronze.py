from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW = "/Volumes/rearc/quest/raw"

@dp.materialized_view(name="rearc.bronze.population")
def bronze_population():
    return (spark.read.option("multiLine", True)
            .json(f"{RAW}/population/population.json")
            .select(F.explode("data").alias("d"))
            .select(
                F.col("d.`Nation ID`").alias("nation_id"),
                F.col("d.Nation").alias("nation"),
                F.col("d.Year").alias("year"),
                F.col("d.Population").alias("population")))


def read_bls_tsv(path):
    df = spark.read.option("sep", "\t").option("header", True).csv(path)
    df = df.toDF(*[c.strip() for c in df.columns])
    return (df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))

@dp.materialized_view(name="rearc.bronze.bls_data")
@dp.expect_or_drop("series_id_not_null", "series_id IS NOT NULL")
def bronze_bls_data():
    return read_bls_tsv(f"{RAW}/bls/pr/pr.data.1.AllData")

@dp.materialized_view(name="rearc.bronze.bls_series")
@dp.expect_or_drop("series_id_not_null", "series_id IS NOT NULL")
def bronze_bls_series():
    return read_bls_tsv(f"{RAW}/bls/pr/pr.series")