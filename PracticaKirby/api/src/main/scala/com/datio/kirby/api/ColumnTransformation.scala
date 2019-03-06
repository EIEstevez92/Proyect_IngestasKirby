package com.datio.kirby.api

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Column transformation api trait.
  * To implement column transformations, extends this trait.
  */
trait ColumnTransformation extends Transformation {

  val transformName: String = config.getString("type")
  val columnName: String = config.getString("field").toColumnName

  /**
    * Method to transform column.
    *
    * @param col to be transformed.
    * @return Column transformed.
    */
  def transform(col: Column): Column

  def transform(df: DataFrame): DataFrame = {
    val columnToTransform = if (df.columns.contains(columnName)) {
      logger.debug(s"Transformer: Use existing column: $columnName")
      df(columnName)
    } else {
      logger.debug(s"Transformer: Create new column  $columnName")
      lit(None.orNull)
    }
    val columnTransformed: Column = transform(columnToTransform)
    df.withColumn(columnName, columnTransformed)
  }

}
