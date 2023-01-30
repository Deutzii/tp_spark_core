import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import when
from pyspark.sql.types import StringType

PATH_JAR = './src/resources/exo4/udf.jar'
PATH_SELL = './src/resources/exo4/sell.csv'

def main():
    spark = SparkSession.builder.appName("exo4").master("local[*]")\
    .appName('scala_udf')\
    .config('spark.jars', 'src/resources/exo4/udf.jar')\
    .getOrCreate()

    df = spark.read.option("header", "true").csv(PATH_SELL)

    df = df.withColumn("category_name", \
        addCategoryName(df["category"], spark)).show()

def addCategoryName(col, spark):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))