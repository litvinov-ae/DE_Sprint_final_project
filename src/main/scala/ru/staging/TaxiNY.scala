package ru.staging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TaxiNY(implicit spark: SparkSession) {

  import spark.implicits._

  private val schema = new StructType()
    .add("VendorID", IntegerType, true)
    .add("tpep_pickup_datetime", TimestampType, true)
    .add("tpep_dropoff_datetime", TimestampType, true)
    .add("passenger_count", IntegerType, true)
    .add("trip_distance", DoubleType, true)
    .add("RatecodeID", IntegerType, true)
    .add("store_and_fwd_flag", StringType, true)
    .add("PULocationID", IntegerType, true)
    .add("DOLocationID", IntegerType, true)
    .add("payment_type", IntegerType, true)
    .add("fare_amount", DoubleType, true)
    .add("extra", DoubleType, true)
    .add("mta_tax", DoubleType, true)
    .add("tip_amount", DoubleType, true)
    .add("tolls_amount", DoubleType, true)
    .add("improvement_surcharge", DoubleType, true)
    .add("total_amount", DoubleType, true)
    .add("congestion_surcharge", DoubleType, true)

  private val df_taxi_with_schema = spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("src/main/resources/yellow_tripdata_2020-01.csv")
    .withColumn("date_trip", substring(col("tpep_pickup_datetime"), 1, 10))


  val df_taxi_percentage = df_taxi_with_schema
    .withColumn("date_trip", substring(col("tpep_pickup_datetime"), 1, 10))
    .groupBy("date_trip")
    .agg(
      round(
        sum(when('passenger_count === 1, 1).otherwise(0))
          / count("*") * 100, 2)
        .as("percentage_1p"),
      round(
        sum(when('passenger_count === 2, 1).otherwise(0))
          / count("*") * 100, 2)
        .as("percentage_2p"),
      round(
        sum(when('passenger_count === 3, 1).otherwise(0))
          / count("*") * 100, 2)
        .as("percentage_3p"),
      round(
        sum(when('passenger_count >= 4, 1).otherwise(0))
          / count("*") * 100, 2)
        .as("percentage_4p_plus"),
      round(
        sum(when('passenger_count >= 4 || 'passenger_count.isNull, 1).otherwise(0))
          / count("*") * 100, 2)
        .as("percentage_zero"),
      min('Fare_amount).as("most_cheap_trip"),
      max('Fare_amount).as("most_expensive_trip"))
    .orderBy("date_trip")
}
