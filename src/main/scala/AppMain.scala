import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object AppMain {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark2.3-3")
      .master("local")
      .getOrCreate()

    val bikeSchema = StructType(Seq(
      StructField("Date", StringType),
      StructField("RENTED_BIKE_COUNT", IntegerType),
      StructField("Hour", IntegerType),
      StructField("TEMPERATURE", DoubleType),
      StructField("HUMIDITY", StringType),
      StructField("WIND_SPEED", StringType),
      StructField("Visibility", StringType),
      StructField("DEW_POINT_TEMPERATURE", StringType),
      StructField("SOLAR_RADIATION", StringType),
      StructField("RAINFALL", StringType),
      StructField("Snowfall", StringType),
      StructField("SEASONS", StringType),
      StructField("HOLIDAYl", StringType),
      StructField("FUNCTIONING_DAY", StringType)
    ))

    val bikeSharingDF = sparkSession.read
      .option("header", "true")
      .option("sep", ",")
      //Сравнение работает корректно только при указании схемы с типами для колонки
      .schema(bikeSchema)
      .csv("src/main/resources/bike_sharing.csv")

    bikeSharingDF
      .groupBy("Date")
      .agg(
        min(column("TEMPERATURE")) as "min_temp",
        max(column("TEMPERATURE")) as "max_temp")
      .show()
  }
}
