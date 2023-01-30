import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App {
  val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Exo 1")
      .getOrCreate();
}

val df = spark.read.load("../ressource/exo1/data.csv")
