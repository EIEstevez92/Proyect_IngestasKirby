package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.util.{Success, Try}

/**
  * Return column with default value.
  *
  * @param config config for literal masterization.
  */
@configurable("caseletter")
class CaseLetter(val config: Config) extends ColumnTransformation {

  lazy val operation: String = Try(config.getString("operation").toLowerCase()) match {
    case Success("upper") => "upper"
    case Success("lower") => "lower"
    case _ => throw new KirbyException("CaseLetter transformation: 'operation' attribute is mandatory (allowed values are 'upper' and 'lower')")
  }

  override def transform(col: Column): Column = {

    logger.info(s"CaseLetter: ${operation}Case to column $columnName")

    operation match {
      case "lower" => lower(col)
      case "upper" => upper(col)
    }
  }


}
