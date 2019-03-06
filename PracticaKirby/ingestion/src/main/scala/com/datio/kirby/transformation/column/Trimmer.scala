package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{ltrim, rtrim, trim, when}

import scala.util.Try

/**
  * Trims a string.
  *
  * @param config     config for trimming masterization.
  */
@configurable("trim")
class Trimmer(val config: Config) extends ColumnTransformation {

  val trimType: String = Try(config.getString("trimType")).getOrElse("both").toLowerCase

  /**
    * Custom trim transformation.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed, if applies.
    */
  override def transform(col: Column): Column = {

    when(col.isNotNull, {
      trimType match {
        case "left" => ltrim(col)
        case "right" => rtrim(col)
        case "both" => trim(col)
        case _ =>
          logger.warn(s"Bad trim configuration: $trimType for column $col. Column won't be trimmed")
          col
      }
    })
  }

}



