package com.datio.kirby.input

import com.datio.kirby.api.implicits.ApplyFormat
import com.datio.kirby.config.configurable
import com.typesafe.config.{Config, ConfigException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * This Class reads a text-fixed file and it return a data frame.
  *
  * @param config config for fixed file input reader.
  */
@configurable("fixed")
class FixedInput(val config: Config) extends InputSchemable with FixedReader with ApplyFormat {

  override def reader(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    logger.info("Input: FixedInput reader")
    val failfast = "failfast"
    val dropMalformed = "dropmalformed"
    val log = logger

    val columnDistribution: Seq[ColLong] = getColumnDistribution(schema)
    val schemaString = schema.map(sh =>
      StructField(sh.name, StringType, nullable = true, sh.metadata))
    val mode: String = Try(config.getString("options.mode").toLowerCase) match {
      case Success(`dropMalformed`) => dropMalformed
      case Success(`failfast`) => failfast
      case Success(m) => throw new RuntimeException(s"FixedInput: mode '$m' not supported")
      case Failure(_: ConfigException.Missing) => failfast
      case Failure(e) => throw new RuntimeException(s"FixedInput: error reading mode parameter", e)
    }

    val rdd = spark
      .sparkContext
      .textFile(path)
      .flatMap(line =>
        Try(columnDistribution.map(colLong => line.substring(colLong.initColumn, colLong.endColumn))) match {
          case Success(parsedLine) => Some(parsedLine)
          case Failure(_) if mode == dropMalformed =>
            log.warn(s"Dropping malformed line: $line")
            None
          case Failure(e) if mode == failfast => throw new RuntimeException(s"Malformed line in FAILFAST mode: $line", e)
        }
      ).map(Row.fromSeq(_))

    spark
      .createDataFrame(rdd, StructType(schemaString))
      .castSchemaFormat(schema)
  }

  def getColumnDistribution(schema: StructType): Seq[ColLong] = {

    val distribution: Seq[ColLong] = schema.tail.foldLeft[Seq[ColLong]](
      Seq(ColLong(0, size(schema.head.metadata.getString("logicalFormat"))))
    )(
      (seqColLong, next) => {
        val initColumnNext = seqColLong.last.endColumn
        seqColLong :+ ColLong(initColumnNext, initColumnNext + size(next.metadata.getString("logicalFormat")))
      }
    )

    logger.debug("Input: FixedInput column Distribution => \n{}",
      distribution.map(colLong => s"init : ${colLong.initColumn} -> end: ${colLong.endColumn}").mkString("\n"))

    distribution
  }

}

trait FixedReader extends LazyLogging {

  import com.datio.kirby.api.LogicalFormatRegex._

  def size(logicalFormat: String): Int = logicalFormat.trim.toUpperCase match {
    case decimalPattern(n, _, null) => n.toInt
    case decimalPattern(_, _, n) => n.toInt
    case decimalPattern2(n, _, _, null) => n.toInt + 2
    case decimalPattern2(_, _, _, n) => n.toInt
    case charPattern(n, _, null) => n.toInt
    case charPattern(_, _, n) => n.toInt
    case numericLargePattern(_, null) => 9
    case numericLargePattern(_, n) => n.toInt
    case numericShortPattern(_, null) => 4
    case numericShortPattern(_, n) => n.toInt
    case datePattern(_, null) => 10
    case datePattern(_, n) => n.toInt
    case timePattern(_, null) => 8
    case timePattern(_, n) => n.toInt
    case timestampPattern(_, null) => 25
    case timestampPattern(_, n) => n.toInt
    case "NUMERIC BIG" | "FLOAT" | "DOUBLE" | "CLOB" | "XML" | _ =>
      logger.error(s"FixedInput: logicalFormal=$logicalFormat is not supported using size 0")
      0
  }
}