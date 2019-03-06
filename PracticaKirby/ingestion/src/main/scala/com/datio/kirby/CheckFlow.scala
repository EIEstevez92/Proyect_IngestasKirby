package com.datio.kirby

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.{Input, Output, Transformation}
import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.errors.{INPUT_GENERIC_ERROR, OUTPUT_GENERIC_ERROR, TRANSFORMATION_GENERIC_ERROR}
import com.datio.kirby.schema.SchemaValidator
import com.datio.spark.metric.utils.ProcessInfo
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait CheckFlow extends LazyLogging with TransformationFactory with SchemaValidator {

  def applySparkOptions(sparkOptions: Map[String, Any])(implicit spark: SparkSession): Unit = {
    sparkOptions.foreach {
      case (k, v: Long) => spark.conf.set(k, v)
      case (k, v: String) => spark.conf.set(k, v)
      case (k, v: Boolean) => spark.conf.set(k, v)
    }
  }

  def readDataFrame(input: Input)(implicit spark: SparkSession): DataFrame = {
    try {
      input.read(spark)
    } catch {
      case e: Exception => {
        logger.error("Exception reading DataFrame: {}", ExceptionUtils.getStackTrace(e))
        e match {
          case ke: KirbyException => throw ke
          case other: Exception => throw new KirbyException(INPUT_GENERIC_ERROR, other)
        }
      }
    }
  }

  def applyTransformations(dfI: DataFrame, transformations: Seq[Transformation])(implicit spark: SparkSession): DataFrame = {
    try {
      transformations.foldLeft(dfI) { (df, transformation) =>
        logger.info(s"Transformer: new Transformation : ${transformation.config.getString("type")}")
        if (Try(transformation.config.getBoolean("regex")).getOrElse(false)) {
          expandTransformation(df.columns, transformation)(spark).
            foldLeft(df)((dfE, tr) => tr.transform(dfE))
        } else {
          transformation.transform(df)
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Exception applying transformations: {}", ExceptionUtils.getStackTrace(e))
        e match {
          case ke: KirbyException => throw ke
          case other: Exception => throw new KirbyException(TRANSFORMATION_GENERIC_ERROR, other)
        }
      }
    }
  }

  def writeDataFrame(df: DataFrame, output: Output, processInfo: ProcessInfo = null): Unit = {
    try {
      output.write(df, processInfo)
    } catch {
      case e: Exception => {
        logger.error("Exception writing DataFrame: {}", ExceptionUtils.getStackTrace(e))
        e match {
          case ke: KirbyException => throw ke
          case other: Exception => throw new KirbyException(OUTPUT_GENERIC_ERROR, other)
        }
      }
    }
  }

}
