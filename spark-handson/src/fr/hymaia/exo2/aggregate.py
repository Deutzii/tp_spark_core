import pyspark.sql.functions as f
from pyspark.sql import SparkSession

PATH_CLIENT_DPT = "data/exo2/clean"

def main():
    spark = SparkSession.builder.master('local[*]') \
    .appName('aggregate') \
    .getOrCreate()

    df_client_dpt = spark.read.parquet(PATH_CLIENT_DPT)

    df_pop_by_dpt = agg_pop_by_departement(df_client_dpt)

    df_pop_by_dpt.write \
        .mode("overwrite") \
        .option("header", True) \
        .parquet("data/exo2/aggregate")

def agg_pop_by_departement(df, dpt_col_name="departement"):
    return df.groupBy(dpt_col_name).count() \
        .withColumnRenamed("count", "nb_people") \
        .sort(f.desc(f.col("nb_people")), f.col(dpt_col_name))

