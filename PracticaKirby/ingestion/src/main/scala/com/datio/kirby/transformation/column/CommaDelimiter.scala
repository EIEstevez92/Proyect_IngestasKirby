package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.udf

@configurable("commadelimiter")
class CommaDelimiter(val config: Config, val spark: SparkSession) extends ColumnTransformation {



  override def transform(col: Column): Column = {

    val lengthDec = Option(config.getInt("lengthDecimal")).getOrElse(2)
    val sepCharacter = Option(config.getString("separatorDecimal")).getOrElse(".")

    val method: java.io.Serializable => String = field => {
      var stringValue = field.toString
      stringValue match {
        case a if stringValue.length > lengthDec =>
          val commaPos = stringValue.length - lengthDec
          stringValue = stringValue.substring(0,commaPos) + sepCharacter + stringValue.substring(commaPos, stringValue.length)
          s"$stringValue"
        case a if stringValue.length.equals(lengthDec) =>
          s"0$sepCharacter$stringValue"
        case a if stringValue.length < lengthDec =>
          val zero = 0
          s"0$sepCharacter${(for (_ <- stringValue.length until lengthDec) yield zero).mkString}$stringValue"
      }
    }

    val leftPaddingUDF = udf(method)
    leftPaddingUDF(col)
  }
}
