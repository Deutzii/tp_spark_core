import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

PATH_SELL = './src/resources/exo4/sell.csv'

def main():
    spark = SparkSession.builder.appName("exo4").master("local[*]")\
    .appName('python_udf') \
    .getOrCreate()

    df = spark.read.option("header", "true").csv(PATH_SELL)
    
    category_name = udf(categorization, StringType())

    df = df.withColumn("category_name", \
        category_name(df["category"])).show()
        

def categorization(cat):
    col = cat.split(" ")
    for c in col : 
        if int(c) < 6:
            return 'food'
        else:
            return 'furniture'

    


