package com.datio.kirby.api

import com.datio.kirby.api.errors.{REPROCESS_EMPTY_LIST, REPROCESS_EMPTY_PARTITION_LIST, REPROCESS_MISSED_PARTITION}
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.implicits.ApplyFormat
import com.datio.kirby.api.util.FileUtils
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Output api trait.
  * To implement outputs, extends this trait.
  */
trait Reprocess extends LazyLogging with ApplyFormat with FileUtils {

  protected implicit val config: Config

  protected val path: String

  protected val partitions: Seq[String]

  protected lazy val reprocess: Seq[String] = Try(config.getStringList("reprocess").asScala).getOrElse(List())

  protected val reprocessMode = "reprocess"

  /**
    * If modeConfig="reprocess" delete the reprocess directories and change de mode to append
    *
    * @param modeConfig write mode pass by configuration
    */
  protected def applyReprocessMode(modeConfig: String): String = {
    modeConfig match {
      case `reprocessMode` =>
        checkReprocessPartition(reprocess, partitions)
        reprocess.toSet[String].foreach(deletePath(path, _))
        "append"
      case _ => modeConfig
    }
  }

  protected def checkReprocessPartition(reprocess: Seq[String], partitions: Seq[String]): Unit = {
    logger.info(s"Output: reprocess partitions ${partitions.mkString(", ")}")
    if (reprocess.isEmpty) {
      throw new KirbyException(REPROCESS_EMPTY_LIST)
    }
    if (partitions.isEmpty) {
      throw new KirbyException(REPROCESS_EMPTY_PARTITION_LIST)
    }
    val partitionRegex = partitions.map(_ + "=[^\\/]*").reduceRight(_ + "(\\/" + _ + ")?") + "\\/?"
    val missed = reprocess.filter(!_.matches(partitionRegex))
    if (missed.nonEmpty) {
      throw new KirbyException(REPROCESS_MISSED_PARTITION, partitions.mkString(", "), missed.mkString(", "))
    }
    val partitionRegexComplete = partitions.map(_ + "=[^\\/]*").reduce(_ + "\\/" + _ + "") + "\\/?"
    val different = reprocess.filter(!_.matches(partitionRegexComplete))
    if (different.nonEmpty) {
      logger.warn(s"Output: There are reprocess that not contains all levels of partition: ${different.mkString(", ")})")
    }
  }

}
