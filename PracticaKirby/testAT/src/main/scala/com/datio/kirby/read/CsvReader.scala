package com.datio.kirby.read

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object CsvReader {

  def read(pathToRead: String): DataFrame = {
    
    val spark = SparkSession
      .builder()
      .appName("Parquet Reader")
      .master("local[4]")
      .getOrCreate()

    Try(
      spark
        .read
        .csv(pathToRead)
    ).getOrElse(spark.emptyDataFrame)
  }

}
