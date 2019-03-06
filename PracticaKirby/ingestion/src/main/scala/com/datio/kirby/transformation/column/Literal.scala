package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.datio.kirby.util.ConversionTypeUtil
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
  * Return column with default value.
  *
  * @param config config for literal masterization.
  */
@configurable("literal")
class Literal(val config: Config) extends ColumnTransformation with ConversionTypeUtil {

  lazy val defaultValue: String = config.getString("default")

  override def transform(col: Column): Column = {

    logger.info("Masterization: Create literal column with default value : {} ", defaultValue)

    val columnLiteralInString = lit(defaultValue)

    castIfNeeded(columnLiteralInString)
  }

  private def castIfNeeded(columnInString: Column) = {

    if (config.hasPath("defaultType")) {
      columnInString.cast(typeToCast(config.getString("defaultType")))
    } else {
      columnInString
    }
  }

}
