package com.datio.kirby

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.GENERIC_LAUNCHER_ERROR
import com.datio.kirby.config.ConfigurationHandler
import com.datio.spark.InitSpark
import com.datio.spark.config.ConfigUtil
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Main entryPoint for launching kirby process.
  * This object implements the necessary logic for running
  * the ingestion process after of initialise the Spark session.
  *
  * @since 0.3
  **/
object Launcher extends CheckFlow with InitSpark {

  override def defineBusinessInfo(config: Config): BusinessInformation = {
    def getEntityFromSchemaPath(schema: String) = {
      import org.apache.commons.io.FilenameUtils
      Try(FilenameUtils.getBaseName(schema)).getOrElse("")
    }

    val exitcode = 0
    val schema: String = Try(config.getString("kirby.output.schema.path")).getOrElse("")
    val entity: String = getEntityFromSchemaPath(schema)
    val path: String = Try(config.getString("kirby.output.path")).getOrElse("")
    val mode: String = Try(config.getString("kirby.output.mode")).getOrElse("")
    val schemaVersion: String = "1.0"
    val reprocessing: String = Try(config.getStringList("kirby.output.reprocess").asScala).getOrElse(List()).mkString(", ")
    BusinessInformation(exitcode, entity, path, mode, schema, schemaVersion, reprocessing)
  }

  /**
    * Main method for running the spark process
    **/
  override def runProcess(sparkT: SparkSession, config: Config): Int =
    Try {
      implicit val spark: SparkSession = sparkT

      logger.info("Init configuration")
      val kirbyConf = ConfigurationHandler.apply(spark, config)

      logger.info("Apply spark options")
      applySparkOptions(kirbyConf.sparkOptions)

      logger.info("Reading input")
      val input = readDataFrame(kirbyConf.input)

      logger.info("Apply transformations")
      val transformedDF = applyTransformations(input, kirbyConf.transformations)

      logger.info("Validate df")
      validateDF(transformedDF, kirbyConf.outputSchema)

      logger.info("Check mandatory columns")
      val validatedDF = applyMandatoryColumns(transformedDF, kirbyConf.outputSchema)

      logger.info("Write the file in the correct format")
      writeDataFrame(validatedDF, kirbyConf.output, processInfo)
    }
    match {
      case Success(_) => 0
      case Failure(ex:Throwable) => {
        logger.warn(s"Input Args: ${config.toString}")
        logger.error("Exception: {}", ExceptionUtils.getStackTrace(ex))
        ex match {
          case kex:KirbyException => throw kex
          case other:Exception => throw new KirbyException(GENERIC_LAUNCHER_ERROR, other)
        }
      }
    }
}
