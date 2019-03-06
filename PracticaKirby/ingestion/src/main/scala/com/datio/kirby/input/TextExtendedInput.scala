package com.datio.kirby.input

import com.datio.kirby.config.configurable
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * This Class reads a file in TextExtended format and it return a data frame.
  *
  * @param config config for TextExtended input reader.
  */
@configurable("text-extended")
class TextExtendedInput(val config: Config) extends InputSchemable with Serializable {

  val failfast = "failfast"
  val dropmalformed = "dropmalformed"

  lazy val rowDelimiter: String = Try(config.getString("delimiterRow")).getOrElse(throw TextExtendedInputException("delimiterRow is mandatory"))
  lazy val columnDelimiter: String = Try(config.getString("delimiter")).getOrElse(throw TextExtendedInputException("delimiter is mandatory"))
  lazy val options: Config = Try(config.getConfig("options")).getOrElse(ConfigFactory.empty())
  lazy val dateFormatString: String = Try(options.getString("dateFormat")).getOrElse("")
  lazy val dateFormat = new java.text.SimpleDateFormat(dateFormatString)

  lazy val mode: String = Try(config.getString("options.mode").toLowerCase) match {
    case Success(`dropmalformed`) => dropmalformed
    case Success(`failfast`) => failfast
    case Success(m) => throw new RuntimeException(s"FixedInput: mode '$m' not supported")
    case Failure(_: ConfigException.Missing) => failfast
    case Failure(e) => throw new RuntimeException(s"FixedInput: error reading mode parameter", e)
  }

  override def reader(spark: SparkSession, path: String, schema: StructType): DataFrame = {

    logger.info("Input: TextExtendedInput reader")
    val modeS = mode
    def createRow(x: String): Option[Row] = {
      Try {
        val splitData = x.split(columnDelimiter, schema.length)
        val castedRow = for {
          (fieldData, fieldType) <- splitData zip schema.fields}
          yield {
            if (fieldData.isEmpty && fieldType.nullable) {
              None.orNull
            } else {
              cast(fieldData, fieldType)
            }
          }
        Some(Row.fromSeq(castedRow))
      } match {
        case Success(a) => a
        case Failure(tex: TextExtendedInputException) => throw tex
        case Failure(rex: RuntimeException) => modeS match {
          case `dropmalformed` => {
            logger.warn(s"Dropping malformed line: $x")
            None
          }
          case `failfast` => throw new RuntimeException(s"Malformed line in FAILFAST mode: $x", rex)
        }
        case Failure(e) => throw e
      }
    }

    def cast(fieldData: String, fieldType: StructField): Any = {
      Try {
        fieldType.dataType match {
          case _: IntegerType => fieldData.toInt
          case _: DoubleType => fieldData.toDouble
          case _: LongType => fieldData.toLong
          case _: FloatType => fieldData.toFloat
          case _: DateType => dateFormat.parse(fieldData)
          case _ => fieldData
        }
      } match {
        case Success(a) => a
        case Failure(e) => throw new RuntimeException(s"Error casting '$fieldData' to '$fieldType'", e)
      }
    }

    val rddAux: RDD[String] = spark.sparkContext.textFile(path)
    val rdd = rddAux.flatMap(_.split(rowDelimiter)).flatMap(createRow)
    spark.sqlContext.createDataFrame(rdd, schema)
  }
}

case class TextExtendedInputException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)