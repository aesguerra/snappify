package org.minyodev

import org.apache.spark.SparkContext
import org.apache.spark.sql._

object SDTest extends App {

  val sc = new SparkContext("local[*]", "SDTest")
  val spark: SparkSession = SparkSession.builder
    .appName("SDTest")
    .master("local[*]")
    .getOrCreate

  val snc = new SnappySession(spark.sparkContext)
  snc.sql("CREATE TABLE IF NOT EXISTS AIRLINE ( " +
    "Year           VARCHAR(25) NOT NULL," +
    "Month          VARCHAR(25) NOT NULL," +
    "DayOfMonth     VARCHAR(25) NOT NULL," +
    "DayOfWeek      VARCHAR(25)," +
    "DepTime        VARCHAR(25)," +
    "CRSDepTime     VARCHAR(25)," +
    "ArrTime        VARCHAR(25)," +
    "CRSArrTime     VARCHAR(25)," +
    "UniqueCarrier  VARCHAR(25)," +
    "FlightNum       VARCHAR(25)," +
    "TailNum        VARCHAR(25)," +
    "ActualElapsedTime  VARCHAR(25)," +
    "CRSElapsedTime VARCHAR(25)," +
    "AirTime        VARCHAR(25)," +
    "ArrDelay       VARCHAR(25)," +
    "DepDelay       VARCHAR(25)," +
    "Origin         VARCHAR(25)," +
    "Dest           VARCHAR(25)," +
    "Distance       VARCHAR(25)," +
    "TaxiIn         VARCHAR(25)," +
    "TaxiOut        VARCHAR(25)," +
    "Cancelled      VARCHAR(25)," +
    "CancellationCode   VARCHAR(25)," +
    "Diverted       VARCHAR(25)," +
    "CarrierDelay   VARCHAR(25)," +
    "WeatherDelay   VARCHAR(25)," +
    "NASDelay       VARCHAR(25)," +
    "SecurityDelay  VARCHAR(25)," +
    "LateAircraftDelay  VARCHAR(25))" +
    "USING COLUMN OPTIONS (PARTITION_BY 'Year,Month,DayOfMonth')")

  val tableSchema = snc.table("AIRLINE").schema
  val airlineDF = snc.read.schema(schema = tableSchema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv("/home/aesguerra/Downloads/flights/2005.csv")
  airlineDF.write.insertInto("AIRLINE")

  val result = snc.sql("SELECT Year,Month,DayOfMonth,FlightNum FROM AIRLINE").take(5)

  result.foreach(println)
}
