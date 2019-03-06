package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This class create or replace a column with current time
  *
  * @param config updateTime Column.
  */
@configurable("setcurrentdate")
class UpdateTime(val config: Config)(val spark: SparkSession) extends RowTransformation {

  private lazy val column = config.getString("field").toColumnName

  override def transform(df: DataFrame): DataFrame = {
    val now = java.sql.Timestamp.from(java.time.Instant.now)
    df.withColumn(column, lit(now))
  }

}
