package com.datio.kirby.read

import org.apache.spark.sql.{DataFrame, SparkSession}

object JSONReader {

  def read(pathToRead : String) : DataFrame = {

    val spark = SparkSession
      .builder()
      .appName("JSON Reader")
      .master("local[4]")
      .getOrCreate()

    spark
      .read
      .json(pathToRead)
  }

}
