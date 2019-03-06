package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.datio.kirby.util.ConversionTypeUtil
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

/**
  * Return column with value of other column
  *
  * @param config config for CopyColumn masterization.
  */
@configurable("copycolumn")
class CopyColumn(val config: Config)(implicit val spark: SparkSession)
  extends ColumnTransformation with ConversionTypeUtil {

  lazy val copyField: String = config.getString("copyField").toColumnName

  override def transform(col: Column): Column = {

    import spark.implicits._

    logger.info("Masterization: Copy value of column : {} to column : {}", copyField, columnName)

    castIfNeeded(Symbol(copyField).as(columnName))
  }

  private def castIfNeeded(columnInString: Column) = {

    Try(
      columnInString.cast(typeToCast(config.getString("defaultType")))
    ).getOrElse(
      columnInString
    )
  }

}
