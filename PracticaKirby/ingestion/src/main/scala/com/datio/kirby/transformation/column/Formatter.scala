package com.datio.kirby.transformation.column

import java.util.Collections

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.datio.kirby.util.ConversionTypeUtil
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Applies string replacements, if applicable, and changes the data type of the column
  *
  * @param config config for formatter masterization.
  */
@configurable("formatter")
class Formatter(val config: Config) extends ColumnTransformation with ConversionTypeUtil {

  lazy val typeToCast: String = config.getString("typeToCast")

  lazy val replacements: Seq[(String, String)] = {
    Try(config.getConfigList("replacements")).getOrElse(Collections.emptyList[Config]())
  }.asScala.map { entry =>
    (entry.getValue("pattern").unwrapped.toString,
      entry.getValue("replacement").unwrapped.toString)
  }

  lazy val replacementsStr: String =
    replacements.map { r => s"""pattern[${r._1}] replaced by [${r._2}]""" }.mkString(", ")

  /**
    * Custom formatter transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {

    logger.info("transform {} for column: {} to cast: {} with these replacements: {}", transformName, columnName,
      typeToCast, replacementsStr)

    replacements
      .foldLeft(col) {
        case (acc, (pattern, replacement)) =>
          regexp_replace(acc, pattern, replacement)
      }
      .cast(typeToCast(typeToCast))

  }
}
