package com.datio.kirby.input

import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * This Class reads a file in csv format and it return a data frame.
  * Uses https://github.com/databricks/spark-csv datasource.
  *
  * Mandatory parameters:
  * - paths: Array with files you want to read.
  *
  * Optional parameters:
  * - delimiter: Delimiter. Default: ,
  *
  * @param config config for csv input reader.
  */
@configurable("csv")
class CsvInput(val config: Config) extends InputSchemable {

  override def reader(spark: SparkSession, path: String, schema: StructType): DataFrame = {

    logger.info("Input: CSVInput reader")

    spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", Try(config.getString("delimiter")).getOrElse(";"))
      .option("mode", "FAILFAST")
      .option("charset", "UTF-8")
      .option("comment", "#") // Set with is the value for comments, by default #
      .option("inferSchema", "false")
      .withOptions
      .schema(schema)
      .load(path)
  }
}
