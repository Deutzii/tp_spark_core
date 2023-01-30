import pyspark.sql.functions as f
from pyspark.sql import SparkSession

PATH_ZIPCODE = "/root/spark-handson/src/resources/exo2/city_zipcode.csv"
PATH_CLIENTS = "/root/spark-handson/src/resources/exo2/clients_bdd.csv"

def main():
    spark = SparkSession.builder.master('local[*]') \
    .appName('spark_clean_job') \
    .getOrCreate()

    df_city = spark.read.option("header", "true").csv(PATH_ZIPCODE)
    df_clients = spark.read.option("header", "true").csv(PATH_CLIENTS)

    df_major = filter_age(df_clients)
    df_major_cities = join_city(df_major, df_city)
    
    df_major_cities_dpt = add_departement(df_major_cities)

    df_major_cities_dpt.show()
    df_major_cities_dpt.write \
        .mode("overwrite") \
        .option("header", True) \
        .parquet("data/exo2/clean")

def filter_age(df):
    return df.filter(f.col("age") >= 18)

def join_city(df, df_city):
    return df.join(df, df_city, "zip", "inner")

def add_departement(df, zip_col="zip", dpt_col_name="departement"):
    df_dpt_raw = df.withColumn(dpt_col_name,
                               f.when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) <= "20190"), "2A") \
                                .when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) > "20190"), "2B") \
                                .otherwise(f.substring(f.col(zip_col), 1, 2)))
    return df_dpt_raw

