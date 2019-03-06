package com.datio.kirby.transformation.column

import java.sql.Date
import java.util.Calendar

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

/**
  * Return column info from date of another column
  *
  * @param config config for CopyColumn masterization.
  */
@configurable("extractinfofromdate")
class ExtractInfoFromDate(val config: Config)(implicit val spark: SparkSession) extends ColumnTransformation {

  override def transform(col: Column): Column = {

    import spark.implicits._

    val logSerializable = logger

    val dateField = config.getString("dateField").toColumnName
    val infoToExtract = config.getString("info").toLowerCase

    val calendar = Calendar.getInstance()

    logger.info("Masterization: Extract info: {} from column : {}, to column: {}",
      infoToExtract, dateField, columnName)

    val method: Date => String = date => {

      Try {
        calendar.setTime(date)

        infoToExtract match {
          case "day" => calendar.get(Calendar.DAY_OF_MONTH).toString
          case "month" => calendar.get(Calendar.MONTH).toString
          case "year" => calendar.get(Calendar.YEAR).toString
        }
      }.getOrElse {
        logSerializable.debug(s"fail in ExtractInfoFromDate; info : $infoToExtract ; field : $date")
        None.orNull
      }
    }

    val udfToApply = udf(method)

    udfToApply(Symbol(dateField))

  }

}
