package com.datio.kirby.api

import com.datio.kirby.api.errors.{OUTPUT_OVERWRITE_BLOCKED, OUTPUT_REPROCESS_ABORTED, OUTPUT_REPROCESS_PARTITION_BLOCKED}
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.implicits.ApplyFormat
import com.datio.kirby.api.util.RepartitionDsl._
import com.datio.spark.metric.utils.ProcessInfo
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Output api trait.
  * To implement outputs, extends this trait.
  */
trait Output extends LazyLogging with ApplyFormat with Reprocess {

  protected implicit val config: Config

  lazy val finalPath: String = {
    val tmpPath = config.getString("path")
    logger.info("Input: Write to path => {} ", tmpPath)
    tmpPath
  }

  lazy val partitions: Seq[String] = Try(config.getStringList("partition").asScala).getOrElse(List())

  lazy val safeReprocess: Boolean = Try(config.getBoolean("safeReprocess")).getOrElse(false)

  lazy val force: Boolean = Try(config.getBoolean("force")).getOrElse(false)

  val dir = "tmp_"

  lazy val externalExtraLines: Int = Try(config.getInt("externalExtraLines")).getOrElse(0)

  lazy val path: String = if (safeReprocess) {
    generateTmpPath()
  } else {
    finalPath
  }

  logger.info("Input: Write to path => {} ", path)

  /**
    * Public method for write dataFrame with correct format output, mode and partition.
    *
    * @param df DataFrame to be saved
    */
  def write(df: DataFrame, pi: ProcessInfo = None.orNull): Unit = {

    checkOutput(path)
    val repartitionedDF = df.applyRepartitionIfNedeed
    val DFWriterWithMode = mode(repartitionedDF)
    val DFWriterPartitioned = partitionBy(DFWriterWithMode)

    writeDF(DFWriterPartitioned)

    if (safeReprocess) {
      logger.info(s"Resprocess data in path $path")
      overwriteReprocessedData(pi)
    }
  }

  protected def overwriteReprocessedData(pi: ProcessInfo): Unit = {
    if (pi.getLinesReaded() - externalExtraLines - pi.getLinesWritten() == 0) {
      logger.info(s"Same number of lines in old and new data processed(${pi.getLinesReaded()})")
      logger.info(s"Deleting files in $finalPath")
      deletePath(finalPath, "")
      logger.info(s"Coping new data in $finalPath")
      renameDirectory(path, s"$finalPath")
    }
    else {
      logger.warn(s"Different number of lines in old(${pi.getLinesReaded()}) and new data processed(${pi.getLinesWritten()})" +
        s"with extra($externalExtraLines). Removing data in $path")
      deletePath(path, "")
      throw new KirbyException(OUTPUT_REPROCESS_ABORTED)
    }
  }


  /**
    * Check if path is allowed
    *
    * @param path path to evaluate
    */
  protected def checkOutput(path: String): Unit = {
    isPartition(path) match {
      case true if force => logger.warn(s"Output: FORCE WRITE PARTITION $path")
      case true => throw new KirbyException(OUTPUT_REPROCESS_PARTITION_BLOCKED, path)
      case false =>
    }
  }

  /**
    * Retrieve from config mode to be save dataFrame.
    *
    * @param df DataFrame to apply mode.
    * @return DataFrameWriter with mode.
    */
  protected def mode(df: DataFrame): DataFrameWriter[Row] = {
    if (config.hasPath("mode")) {
      val modeConfig = config.getString("mode").toLowerCase()
      logger.info(s"Output: Mode $modeConfig")
      checkOverwriteMode(modeConfig)
      val finalMode = applyReprocessMode(modeConfig)
      df.write.mode(finalMode)
    } else {
      logger.info("Output: Mode without mode")
      df.write
    }
  }

  /**
    * Retrieve from config partitions to be save dataframe.
    *
    * @param dfw DataFrameWriter to apply partition.
    * @return DataFrameWriter with partition.
    */
  protected def partitionBy(dfw: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    if (partitions.nonEmpty) {
      dfw.partitionBy(partitions: _*)
    } else {
      logger.info("Output: Without Partition")
      dfw
    }
  }

  /**
    * If modeConfig="overWrite" Evaluate output params like path and force to log warnings or not allowed operations.
    *
    * @param modeConfig write mode pass by configuration
    */
  protected def checkOverwriteMode(modeConfig: String): Unit = {
    val overwrite = "overwrite"
    modeConfig match {
      case `overwrite` if force =>
        logger.warn(s"Output: FORCE OVERWRITE COMPLETE DATASET $path")
      case `overwrite` => throw new KirbyException(OUTPUT_OVERWRITE_BLOCKED, overwrite, path)
      case _ =>
    }
  }

  /**
    * read a path and check if target a partition or not
    *
    * @return true if a partition false ioc
    */
  protected def isPartition(path: String): Boolean = path.matches(".*\\/[^\\/]+=[^\\/]*\\/?")

  /**
    * Abstract method for implements in differents formats outputs.
    *
    * @param dfw DataframeWriter to be save in correct format
    */
  protected def writeDF(dfw: DataFrameWriter[Row]): Unit

  /**
    * Utility for add options from config file. Options have to be setted like:
    * output {
    * type : "avro"
    * options {
    * codec = "lzo"
    * recordName = "recordName"
    * }
    * }
    *
    * @param dfw DataFrameWriter that will be apply options.
    */
  protected implicit class DataFrameWriterOptions(dfw: DataFrameWriter[Row]) {
    def withOptions: DataFrameWriter[Row] = {

      if (config.hasPath("options")) {
        val optionsConfig = config.getConfig("options")
        val entries = optionsConfig.entrySet().asScala.toList

        entries.map(_.getKey).foldLeft(dfw)((dfr, key) => dfr.option(key, optionsConfig.getString(key)))
      } else {
        dfw
      }
    }
  }

  protected def generateTmpPath(): String = {
    FilenameUtils.getFullPathNoEndSeparator(finalPath) + "/" + dir + FilenameUtils.getName(finalPath)
  }

}
