import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.window import Window

PATH_SELL = './src/resources/exo4/sell.csv'

def main():
    spark = SparkSession.builder.appName("exo4").master("local[*]")\
    .appName('no_udf') \
    .getOrCreate()
    
    df = spark.read.option("header", "true").csv(PATH_SELL)
    df = df.withColumn("category_name", \
        when((f.col("category") < 6), "food") \
            .otherwise("furniture") \
                )

    df = df.withColumn("price",f.col("price").cast("double"))
    
    windowSpec  = Window.orderBy("date", "category_name")

    sumPrice = sum("price").over(windowSpec)

    df = df.withColumn("total_price_per_category_per_day", sumPrice) \
        
    df.show()
    
    #df = df.groupBy(["date", "category_name"]) \
    #    .agg(sum("price")).alias("total_price_per_category_per_day")

    #df.show()


