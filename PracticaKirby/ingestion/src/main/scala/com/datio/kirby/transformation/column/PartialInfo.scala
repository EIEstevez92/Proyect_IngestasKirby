package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Create new column with partial info of other column.
  *
  * @param config config for partial info masterization.
  */
@configurable("partialinfo")
class PartialInfo(val config: Config)(implicit val spark: SparkSession) extends ColumnTransformation {

  lazy private val startInfo = config.getInt("start")

  lazy private val lengthInfo = config.getInt("length")

  lazy private val fieldInfo = config.getString("fieldInfo")

  override def transform(col: Column): Column = {

    import spark.implicits._

    logger.info("Masterization: create new partial info column from : {}, start in {} and length: {} ",
      fieldInfo, String.valueOf(startInfo), String.valueOf(lengthInfo))

    substring(Symbol(fieldInfo).cast(StringType), startInfo, lengthInfo)

  }
}
