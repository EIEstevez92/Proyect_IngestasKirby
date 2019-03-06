package com.datio.kirby.examples.transform

import com.datio.kirby.api.ColumnTransformation
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Transform string column to upper.
  */
class ToUpperColumnTransformation(val config: Config, val spark: SparkSession) extends ColumnTransformation {

  /**
    * Method transform column.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  def transform(col: Column): Column = {

    val logSerializable = logger

    val methodToUpper: String => String = input => {
      if (input.isEmpty) {
        logSerializable.warn("fail ToUpperTransform, field is Empty")
        ""
      } else {
        input.toUpperCase()
      }
    }

    val udfToApply: UserDefinedFunction = udf(methodToUpper)

    udfToApply(col)
  }

}
