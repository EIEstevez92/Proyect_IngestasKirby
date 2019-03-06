package com.datio.kirby.api.implicits

import java.sql.Timestamp
import java.text.{DecimalFormat, NumberFormat}
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.{LocalDateTime, ZoneId}
import java.util.Locale

import com.datio.kirby.api.errors.APPLY_FORMAT_INVALID_CASTING
import com.datio.kirby.api.exceptions.KirbyException
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import com.datio.kirby.api.CustomFixedLocaleRegex._

import scala.util.matching.Regex
import scala.util.{Success, Try}

/** Apply format dataFrame util to cast columns types
  */
trait ApplyFormat extends LazyLogging {

  /** implicit class used to cast dataFrame column types
    *
    * @param df input dataFrame
    */
  implicit class ApplyFormatUtil(df: DataFrame) extends Serializable {

    /** Cast types of columns of input dataFrame to type of structType pass by parameter
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castSchemaFormat(structType: StructType): DataFrame = {
      castDfToSchema(df, structType)
    }

    /** Cast types of columns of input dataFrame that are Date on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeDate(structType: StructType): DataFrame = {
      castDfToOriginTypeDate(df, structType)
    }

    /** Cast types of columns of input dataFrame that are Decimal on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeDecimal(structType: StructType): DataFrame = {
      castDfToOriginTypeDecimal(df, structType)
    }

    /** Cast types of columns of input dataFrame that are Timestamp on 'logicalFormat' metadata
      *
      * @param structType used to get types to cast dataFrame columns
      */
    def castOriginTypeTimestamp(structType: StructType): DataFrame = {
      castDfToOriginTypeTimestamp(df, structType)
    }

  }

  import com.datio.kirby.api.LogicalFormatRegex._

  private def castDfToOriginTypeDecimal(df: DataFrame, schema: Seq[StructField]): DataFrame = {
    import com.datio.kirby.api.LogicalFormatRegex._
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString("logicalFormat")) match {
          case Success(decimalPattern2(precision, scale, _, _)) =>
            df.withColumn(field.name, castColumnToDecimal(df, field, precision.toInt, scale.toInt).as(field.name, field.metadata))
          case Success(decimalPattern(precision, _, _)) =>
            df.withColumn(field.name, castColumnToDecimal(df, field, precision.toInt, 0).as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  private def castDfToOriginTypeTimestamp(df: DataFrame, schema: Seq[StructField]): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString("logicalFormat")) match {
          case Success(timestampPattern(_, _)) => df.withColumn(field.name, castColumnToTimestamp(df, field).as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  private def castDfToOriginTypeDate(df: DataFrame, schema: Seq[StructField]): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString("logicalFormat")) match {
          case Success(datePattern(_, _)) => df.withColumn(field.name, castColumnToDate(df, field).as(field.name, field.metadata))
          case _ => df
        }
      }
  }

  private def castDfToSchema(dfToApplyCast: DataFrame, schema: Seq[StructField]): DataFrame = {
    schema.foldLeft(dfToApplyCast) {
      (df, field) =>
        if (!hasColumnCorrectDataType(df.schema, field)) {
          val columnCasted = df(field.name).cast(field.dataType)
          df.withColumn(field.name, columnCasted.as(field.name, field.metadata))
        } else {
          df
        }
    }
  }

  private def hasColumnCorrectDataType(dfSchema: StructType, fieldToCast: StructField): Boolean = {
    dfSchema.fields.find(_.name == fieldToCast.name).forall(_.dataType == fieldToCast.dataType)
  }

  private def castColumnToTimestamp(df: DataFrame, field: StructField): Column = {
    df.schema.filter(_.name == field.name).head.dataType match {
      case StringType =>
        val format = getFormat(field)
        val locale = getLocale(field)
        val dateParser = udf((date: String) => {
          Try {
            val formatterBuilder = new DateTimeFormatterBuilder()
              .parseCaseInsensitive()
              .appendPattern(format.getOrElse(""))
              .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
              .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
              .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
              .parseDefaulting(ChronoField.INSTANT_SECONDS, 0)
            val formatter = locale.map(formatterBuilder.toFormatter).getOrElse(formatterBuilder.toFormatter)
            Some(new Timestamp(LocalDateTime.from(formatter.parse(date)).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli))
          }.getOrElse(None)
        })
        dateParser(df(field.name)).cast(TimestampType)
      case otherValid if Cast.canCast(otherValid, TimestampType) => df(field.name).cast(TimestampType)
      case otherInvalid =>
        throw new KirbyException(APPLY_FORMAT_INVALID_CASTING, field.name, s"$otherInvalid", "TimestampType / DateType")
    }
  }

  private def castColumnToDate(df: DataFrame, field: StructField): Column = {
    to_date(castColumnToTimestamp(df, field))
  }

  private def fixedCustomDecimalParser(typeSigned: String, scale: Int) = udf((signedNumber: String) => {
    val (number,sign) = typeSigned match {
      case `fixedSignedLeft` => (signedNumber.substring(1), signedNumber.substring(0,1))
      case `fixedSignedRight` => val length = signedNumber.length()
        (signedNumber.substring(0,length - 1), signedNumber.substring(length - 1))
      case `fixedUnsigned` => (signedNumber, positiveSign)
      case _ =>
        throw new KirbyException(APPLY_FORMAT_INVALID_CASTING, typeSigned, signedNumber)
    }
    val integerPart = number.substring(0,number.length() - scale)
    val fractionalPart = number.substring(number.length() - scale)
    s"$sign$integerPart.$fractionalPart"
  })

  private def castColumnToDecimal(df: DataFrame, field: StructField, precision: Int, scale: Int): Column = {
    logger.info("ApplyFormatUtil: DecimalConversion -> Change the precision / scale in a given decimal to those set " +
      "in `decimalType` (if any), returning null if it overflows or modifying `value` in-place and returning it if successful.")
    df.schema.filter(_.name == field.name).head.dataType match {
      case StringType =>
        def createNumberFormatter(field: StructField): Option[DecimalFormat] = {
          getLocale(field).map(locale => {
            val formatter = NumberFormat.getInstance(locale).asInstanceOf[DecimalFormat]
            formatter.setParseBigDecimal(true)
            formatter
          })
        }
        Try(field.metadata.getString("locale")) match {
          case Success(fixedSignedLeftPattern()) =>
            fixedCustomDecimalParser(fixedSignedLeft, scale)(df(field.name)).cast(DecimalType(precision, scale))
          case Success(fixedSignedRightPattern()) =>
            fixedCustomDecimalParser(fixedSignedRight, scale)(df(field.name)).cast(DecimalType(precision, scale))
          case Success(fixedUnsignedPattern()) =>
            fixedCustomDecimalParser(fixedUnsigned, scale)(df(field.name)).cast(DecimalType(precision, scale))
          case _ =>
            createNumberFormatter(field) match {
              case Some(numberFormatter) =>
                val decimalParser = udf((number: String) => {
                  Try(Some(numberFormatter.parse(number).asInstanceOf[java.math.BigDecimal])).getOrElse(None)
                })
                decimalParser(df(field.name)).cast(DecimalType(precision, scale))
              case None =>
                df(field.name).cast(DecimalType(precision, scale))
            }
        }

      case otherValid if Cast.canCast(otherValid, DecimalType(precision, scale)) =>
        df(field.name).cast(DecimalType(precision, scale))
      case otherInvalid =>
        throw new KirbyException(APPLY_FORMAT_INVALID_CASTING, field.name, s"$otherInvalid", s"DecimalType($precision, $scale)")
    }
  }

  private def getFormat(field: StructField): Option[String] = {
    Try(field.metadata.getString("format")) match {
      case Success(format) => Some(format)
      case _ => None
    }
  }

  private def getLocale(field: StructField): Option[Locale] = {
    val localeMatcher: Regex = """([a-z,A-Z]*)[_-]([a-z,A-Z]*)""".r
    Try(field.metadata.getString("locale")) match {
      case Success((localeMatcher(lang, country))) =>
        Some(new Locale(lang, country))
      case Success(lang) =>
        Some(new Locale(lang))
      case _ => None
    }
  }
}