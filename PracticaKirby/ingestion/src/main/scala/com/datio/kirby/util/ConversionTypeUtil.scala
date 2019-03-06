package com.datio.kirby.util

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.CONVERSION_TYPE_ERROR
import org.apache.spark.sql.types._

trait ConversionTypeUtil {

  /**
    * Hardcoded parsed file from different formats.
    *
    * @param dataType format from the schema file
    * @return Spark data type.
    */
  def typeToCast(dataType: String): DataType = {
    val decimalMatcher = """^decimal *\( *(\d+) *, *(\d+) *\)$""".r
    val decimalOnlyPrecisionMatcher = """^decimal *\( *(\d+) *\)$""".r

    dataType.trim.toLowerCase match {
      case "string" => StringType
      case "int32" | "int" | "integer" => IntegerType
      case "int64" | "long" => LongType
      case "double" => DoubleType
      case "float" => FloatType
      case "boolean" => BooleanType
      case "date" => DateType
      case "timestamp_millis" => TimestampType
      case decimalMatcher(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case decimalOnlyPrecisionMatcher(precision) => DecimalType(precision.toInt, 0)
      case other: String => throw new KirbyException(CONVERSION_TYPE_ERROR, other)
    }
  }

}
