import pyspark.sql.functions as f
from pyspark.sql import SparkSession

path_data = "/root/spark-handson/src/resources/exo1/data.csv"
path_output = "data/exo1/output"

def main():
    spark = SparkSession.builder.master('local[*]') \
    .appName('wordcount') \
    .getOrCreate()

    df = spark.read.option("header", "true").csv(path_data)
    df = wordcount(df, "text")
    df.show()
    df.write.csv("outputs")

    print("Hello world!")


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
