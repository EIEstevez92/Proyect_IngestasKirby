package com.datio.kirby.examples.input

import com.datio.kirby.api.Input
import com.typesafe.config.Config
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Dictionary Input.
  * Read text file and transform in dataframe of key value.
  */
class DictionaryInput(val config : Config) extends Input {


  override protected def reader(spark: SparkSession) (path : String): DataFrame = {

    val rdd = spark
      .sparkContext
      .textFile(path)
      .map(_.split(","))
      .map(keyValue => Row(keyValue(0), keyValue(1)))

    spark.createDataFrame(rdd,
      StructType(Seq(
        StructField("key", StringType),
        StructField("value", StringType)
      ))
    )

  }

}

