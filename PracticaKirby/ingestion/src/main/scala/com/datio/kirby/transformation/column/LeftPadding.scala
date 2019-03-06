package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

/**
  * Cast a number to alphanumeric and preprend the fillCharacter until the length desired.
  * Parameters:
  *  - lengthDest (integer)
  *  - fillCharacter (string)
  *
  * The defaults are:
  * {
  * lengthDest = 4
  * fillCharacter = "0"
  * }
  *
  * @param config config for leftpadding masterization.
  */
@configurable("leftpadding,toalphanumeric")
class LeftPadding(val config: Config, val spark: SparkSession) extends ColumnTransformation {

  override def transform(col: Column): Column = {

    val lengthDest = Option(config.getInt("lengthDest")).getOrElse(4)
    val fillCharacter = Option(config.getString("fillCharacter")).getOrElse("0")
    val nullValue = Try(config.getString("nullValue")).getOrElse(null)

    val method: java.io.Serializable => String = {
      case null => nullValue
      case field: Any =>
        val stringValue = field.toString
        s"${(for (_ <- stringValue.length until lengthDest) yield fillCharacter).mkString}$stringValue"
    }

    val leftPaddingUDF = udf(method)
    leftPaddingUDF(col)
  }

}
