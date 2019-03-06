package com.datio.kirby.api

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Transformation api trait.
  * To implement transformations, extends ColumnTransformation or RowTransformation.
  */
trait Transformation extends LazyLogging {

  val config: Config

  /**
    * Method to perform a transformation.
    *
    * @param df DataFrame to be transformed.
    * @return DataFrame transformed.
    */
  def transform(df: DataFrame): DataFrame

  /**
    * When the column name contains strange characters, the configuration library returns it enclosed in quotation marks.
    *
    * @param columnName column name
    */
  implicit class ColumnNameParser(columnName: String) {
    /** This method removes those quotes so spark can find the column correctly* Transform '"columnName"' into 'columnName'
      *
      * @return column name without being enclosed in quotation marks
      */
    def toColumnName: String = columnName.replaceAll("^\"|\"$", "")
  }

}
