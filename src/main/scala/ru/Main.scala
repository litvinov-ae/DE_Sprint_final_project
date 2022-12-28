package ru

import org.apache.spark.sql.SparkSession
import ru.staging.TaxiNY


object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkApp")
      .getOrCreate()

    new TaxiNY().df_taxi_percentage
      .coalesce(1)
      .write.parquet("src/main/resources/output")

    spark.read
      .parquet("src/main/resources/output/part-00000-796ce024-3e8f-4b98-919e-2f90aac628db-c000.snappy.parquet")
      .createOrReplaceTempView("table_taxi_percentage")

    spark.close()
  }
}
