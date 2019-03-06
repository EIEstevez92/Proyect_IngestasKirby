package com.datio.kirby.config

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.{Input, Output, Transformation}
import com.datio.kirby.config.validator.ConfigValidator
import com.datio.kirby.constants.ConfigConstants._
import com.datio.kirby.errors.{CONFIGURATION_GENERIC_ERROR, ErrorManager}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Try

object ConfigurationHandler extends ConfigurationHandler

trait ConfigurationHandler extends LazyLogging
  with InputFactory
  with OutputFactory
  with SchemaReader
  with SparkOptionReader
  with TransformationFactory {

  val HDFS_LIT = "hdfs"

  var outputSchema: StructType = StructType(Seq())

  /**
    * Initialize configuration.
    *
    * @param spark      SparkSession to retrieve config file
    * @param rootConfig Config main app
    * @return ConfigurationHandler initialized
    */
  def apply(spark: SparkSession, rootConfig: Config): KirbyConfig = {
    try {
      logger.info("Init config structure validation")
      ConfigValidator(rootConfig)

      val config = rootConfig.getConfig(ROOT_CONF_KEY)
      ErrorManager.checkErrorConfiguration(config)

      logger.debug(s"New configuration created : ${config.root().render()}")

      val inputConf = config.getConfig(INPUT_CONF_KEY)
      val outputConf = config.getConfig(OUTPUT_CONF_KEY)
      val outputSchemaConf = config.getConfig(s"$OUTPUT_CONF_KEY.$SCHEMA_CONF_KEY")
      val transformationConfigs = Try(config.getConfigList(TRANSFORMATION_CONF_KEY).asScala).getOrElse(Seq())

      val input: Input = readInput(inputConf)
      val output: Output = readOutput(outputConf)
      outputSchema = readSchema(outputSchemaConf, includeMetadata = true)
      val transformations: Seq[Transformation] = transformationConfigs.map(transformationConfig => readTransformation(transformationConfig)(spark))
      val sparkOptions: Map[String, Any] = readSparkOptions(config, SPARK_OPTIONS)

      KirbyConfig(input, output, outputSchema, transformations, sparkOptions)
    } catch {
      case e: Exception =>
        logger.error(s"Exception: ${ExceptionUtils.getStackTrace(e)}")
        e match {
          case ke: KirbyException => throw ke
          case other: Exception => throw new KirbyException(CONFIGURATION_GENERIC_ERROR, other)
        }
    }

  }

}
