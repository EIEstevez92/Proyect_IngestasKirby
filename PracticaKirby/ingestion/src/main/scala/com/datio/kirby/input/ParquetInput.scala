package com.datio.kirby.input

import com.datio.kirby.api.Input
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This Class reads a file in parquet format and it return a data frame.
  *
  * @param config config for parquet input reader.
  */

@configurable("parquet")
class ParquetInput(val config: Config) extends Input {

  override def reader(spark: SparkSession)(path: String): DataFrame = {

    logger.info("Input: ParquetInput reader")

    spark
      .read
      .withOptions
      .parquet(path)
  }
}
