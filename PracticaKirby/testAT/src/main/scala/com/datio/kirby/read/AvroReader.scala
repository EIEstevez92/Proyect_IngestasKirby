package com.datio.kirby.read

import com.databricks.spark.avro.AvroDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object AvroReader {

  def read(pathToRead : String) : DataFrame = {

    val spark = SparkSession.builder().appName("Avro Reader").master("local[4]").getOrCreate()

    spark
      .read
      .avro(pathToRead)
  }

}
