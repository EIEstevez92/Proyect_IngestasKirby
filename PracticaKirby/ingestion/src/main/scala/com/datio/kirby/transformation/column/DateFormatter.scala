package com.datio.kirby.transformation.column

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  * Apply SimpleDateFormat from String->Date/Timestamp, Date/Timestamp->String and String->String
  *
  * @param config config for date formatter transformation.
  */
@configurable("dateformatter")
class DateFormatter(val config: Config) extends ColumnTransformation {

  lazy val format: String = config.getString("format")
  lazy val reformat: Option[String] = Try(Some(config.getString("reformat"))).getOrElse(None)

  lazy val locale: Locale = getLocale("locale")
  lazy val reLocale: Locale = getLocale("relocale")

  private def getLocale(key: String) = {
    val localeMatcher: Regex = """([a-z,A-Z]*)[_-]([a-z,A-Z]*)""".r
    Try(config.getString(key)) match {
      case Success(localeMatcher(lang, country)) => new Locale(lang, country)
      case Success(lang) => new Locale(lang)
      case _ => new Locale("en")
    }
  }

  val reformatOperation = "reformat"
  val formatOperation = "format"
  val parseDateOperation = "parse"
  val parseTimestampOperation = "parseTimestamp"

  lazy val operation: String = Try(config.getString("operation")) match {
    case Success(`formatOperation`) => formatOperation
    case Success(`parseDateOperation`) => parseDateOperation
    case Success(`parseTimestampOperation`) => parseTimestampOperation
    case Success(`reformatOperation`) if reformat.isDefined => reformatOperation
    case Failure(_: ConfigException.Missing) => parseDateOperation
    case Success(`reformatOperation`) if reformat.isEmpty => throw new RuntimeException("DateFormatter: for reformat operation reformat value is mandatory")
    case _ => throw new RuntimeException(s"DateFormatter: operation is not allowed." +
      s" Available operations are $formatOperation, $parseDateOperation, $formatOperation")
  }

  /**
    * Apply date format.
    *
    * @param col column to be transformed.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {
    val formatter: SimpleDateFormat = new SimpleDateFormat(format, locale)
    operation match {
      case `formatOperation` =>
        logger.info(s"DateFormatter: apply format format operation with '$format' to column $columnName")
        formatDate(col, formatter)
      case `parseDateOperation` =>
        logger.info(s"DateFormatter: apply parse operation to date with '$format' to column $columnName")
        parseDate(col, formatter)
      case `parseTimestampOperation` =>
        logger.info(s"DateFormatter: apply parse operation to timestamp with '$format' to column $columnName")
        parseTimestamp(col, formatter)
      case `reformatOperation` =>
        logger.info(s"DateFormatter: apply reformat operation with '$format'->'${reformat.get}' to column $columnName")
        reformatDate(col, formatter)
    }
  }

  private def formatDate(col: Column, formatter: SimpleDateFormat) = {
    val logSerializable = logger
    val formatDate = udf((date: Timestamp) => {
      Try(formatter.format(date)).getOrElse {
        logSerializable.error(s"DateFormatter: Error applying format operation to $date with '${formatter.toPattern}'")
        None.orNull
      }
    })
    formatDate(col).cast(StringType)
  }

  private def parseDate(col: Column, formatter: SimpleDateFormat) = {
    val logSerializable = logger
    val parseDate = udf((dateInString: String) => {
      Try(new Date(formatter.parse(dateInString).getTime)).getOrElse {
        logSerializable.error(s"DateFormatter: Error applying parse operation to $dateInString with '${formatter.toPattern}'")
        None.orNull
      }
    })
    parseDate(col).cast(DateType)
  }

  private def parseTimestamp(col: Column, formatter: SimpleDateFormat) = {
    val logSerializable = logger
    val parseDate = udf((dateInString: String) => {
      Try(new Timestamp(formatter.parse(dateInString).getTime)).getOrElse {
        logSerializable.error(s"DateFormatter: Error applying parse operation to $dateInString with '${formatter.toPattern}'")
        None.orNull
      }
    })
    parseDate(col).cast(TimestampType)
  }

  private def reformatDate(col: Column, formatter: SimpleDateFormat) = {
    val logSerializable = logger
    val reFormatter: Option[SimpleDateFormat] = reformat.map(new SimpleDateFormat(_, reLocale))
    val reFormatDate = udf((dateInString: String) => {
      Try(reFormatter.get.format(formatter.parse(dateInString))).getOrElse {
        logSerializable.error(s"DateFormatter: Error applying reformat operation to $dateInString with '${formatter.toPattern}'->'${reFormatter.get.toPattern}'")
        None.orNull
      }
    })
    reFormatDate(col)
  }

}
