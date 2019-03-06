package com.datio.kirby.api.util

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.errors.REPARTITION_NO_PARTITION_PARAMETER
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.util.Try

object RepartitionsFunctions {
  val REPARTITION_SUBCONFIG = "repartition"
  val COALESCE_SUBCONFIG = "coalesce"
  val PARTITIONS_OPTION = "partitions"
  val PARTITION_BY_OPTION = "partitionBy"
}

class RepartitionsFunctions(self: DataFrame) extends LazyLogging {

  import RepartitionsFunctions._

  /**
    * Apply a repartition to the dataframe if it exists in the configuration.
    *
    * @param config Config.
    * @return Repartitioned Dataframe.
    */
  def applyRepartitionIfNedeed()(implicit config: Config): DataFrame = {
    if (config.hasPath(REPARTITION_SUBCONFIG)) {
      val repConfig = config.getConfig(REPARTITION_SUBCONFIG)
      val partitions = Try(repConfig.getInt(PARTITIONS_OPTION)).toOption
      val partitionBy = Try(repConfig.getStringList(PARTITION_BY_OPTION)).toOption.map(_.asScala.toList)

      if (partitions.isDefined && partitionBy.isDefined) {
        val columns = partitionBy.get.foldLeft(Array[Column]())((array, column) => array :+ self.col(column))
        logger.debug("Repartitioning input Dataframe by {} partitions and {} columns.", partitions, partitionBy.get.mkString(","))
        self.repartition(partitions.get, columns: _*)
      } else if (partitions.isDefined) {
        logger.debug("Repartitioning input Dataframe by {} partitions.", partitions.get)
        self.repartition(partitions.get)
      } else if (partitionBy.isDefined) {
        val columns = partitionBy.get.foldLeft(Array[Column]())((array, column) => array :+ self.col(column))
        logger.debug("Repartitioning input Dataframe by {} columns.", partitionBy.get.mkString(","))
        self.repartition(columns: _*)
      } else {
        logger.debug(s"No repartition applied to input. Number of partitions: ${self.rdd.getNumPartitions}")
        self
      }
    } else if (config.hasPath(COALESCE_SUBCONFIG)) {
      val coalesceConfig = config.getConfig(COALESCE_SUBCONFIG)
      val partitions = Try(coalesceConfig.getInt(PARTITIONS_OPTION)).toOption
      if (partitions.isDefined) {
        logger.debug("Coalescing input Dataframe to {} partitions.", partitions.get)
        self.coalesce(partitions.get)
      } else {
        throw new KirbyException(REPARTITION_NO_PARTITION_PARAMETER)
      }
    } else {
      logger.debug(s"No repartition applied to input. Number of partitions: ${self.rdd.getNumPartitions}")
      self
    }
  }

}


trait RepartitionDslFunctions {

  implicit def repartitionFunctions(df: DataFrame): RepartitionsFunctions = new RepartitionsFunctions(df)
}

object RepartitionDsl extends RepartitionDslFunctions