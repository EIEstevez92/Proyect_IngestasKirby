package com.datio.kirby.api

import com.datio.kirby.api.errors.INPUT_PATHS_NOT_SET
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.util.RepartitionDsl._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Input api trait.
  * To implement inputs, extends this trait.
  */
trait Input extends LazyLogging {

  protected implicit val config: Config

  protected lazy val paths: List[String] = Try(config.getStringList("paths").asScala.toList).getOrElse(List())

  /**
    * Read data for any source and return spark data frame
    *
    * @param spark to read data
    * @return
    */
  def read(spark: SparkSession): DataFrame = {

    if (paths.isEmpty) {
      throw new KirbyException(INPUT_PATHS_NOT_SET)
    }

    // Include hadoop configurations if needed
    setHadoopOptions(spark)

    logger.info("Input: Init read")

    val dfRead: DataFrame = paths.tail.foldLeft[DataFrame](
      reader(spark)(paths.head)
    )(
      (dfOrigin, nextPath) => {
        dfOrigin.union(reader(spark)(nextPath))
      }
    )

    logger.info("Input: Read from paths => {} ", paths.mkString(","))

    dfRead
      .applyRepartitionIfNedeed
  }

  /**
    * Utility for check if need to add hadoop configuration in SparkContext
    *
    */
  private def setHadoopOptions(spark: SparkSession): Unit = {
    if (config.hasPath("hadoop")) {

      val optionsConfig = config.getConfig("hadoop")
      val entries = optionsConfig.entrySet().asScala.toList
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      entries.map(_.getKey).foreach(key => hadoopConf.set(key, optionsConfig.getString(key)))
    }
  }

  protected def reader(spark: SparkSession)(path: String): DataFrame

  /**
    * Utility for adding options from config file. Options have to be set like:
    * input {
    * type : "avro"
    * options {
    * codec = "lzo"
    * inferSchema = "true"
    * }
    * }
    *
    * @param dfReader DataFrameReader that will be apply options.
    */
  protected implicit class DataFrameReaderOptions(dfReader: DataFrameReader) {
    def withOptions: DataFrameReader = {

      if (config.hasPath("options")) {

        val optionsConfig = config.getConfig("options")
        val entries = optionsConfig.entrySet().asScala.toList

        logger.info(s"Input: with options: ${entries.map(entry => s"${entry.getKey} -> ${entry.getValue}").mkString(";")}")
        entries.map(_.getKey).foldLeft(dfReader)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
      } else {
        logger.info("Input: without options:")
        dfReader
      }
    }
  }

}

