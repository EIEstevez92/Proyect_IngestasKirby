package com.datio.kirby.input

import com.datio.kirby.api.Input
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.configurable
import com.datio.kirby.errors.INPUT_XMLINPUT_COLUMN_MAPPING_PATH_ERROR
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * This Class reads a file in xml format and it return a data frame.
  * Uses https://github.com/databricks/spark-xml datasource.
  *
  * Mandatory parameters:
  * - paths: Array with files you want to read.
  *
  * Optional parameters:
  * - delimiter: Delimiter. Default: ,
  *
  * @param config config for xml input reader.
  */
@configurable("xml")
class XmlInput(val config: Config) extends InputSchemable {

  implicit class XmlDataFrameReader(reader: DataFrameReader) {
    def xml: String => DataFrame = reader.format("com.databricks.spark.xml").load
  }

  private val inferColumns = Try(config.getBoolean("columnMapping.inferColumns")).getOrElse(true)

  private val columnsMappingPath = Try(config.getString("columnMapping.path")) match {
    case Success(path) => Some(path)
    case Failure(_: ConfigException.Missing) => None
    case Failure(e) => throw new KirbyException(INPUT_XMLINPUT_COLUMN_MAPPING_PATH_ERROR, e)

  }

  override def reader(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    val log = logger
    log.info("Input: XMLInput reader")

    val inferedColumns = if (inferColumns) {
      spark.read.withOptions.xml(path).columns.map(col => (toPowerCenterName(col), col)).toMap
    } else {
      Map()
    }
    val userMappedColums = columnsMappingPath.map(readColumnMapping(_, spark)).getOrElse(Map())
    val inputColumnsMap = inferedColumns ++ userMappedColums

    val inputColumnNameSchema = StructType(
      schema.map(structField => {
        inputColumnsMap.get(structField.name.toLowerCase) match {
          case Some(inputColumnName) if structField.name != inputColumnName =>
            StructField(
              inputColumnName,
              structField.dataType,
              structField.nullable,
              structField.metadata)
          case Some(inputColumnName) if structField.name == inputColumnName => structField
          case None =>
            log.warn(s"Input: XMLInput reader. Column ${structField.name} not located on schema")
            structField
        }

      }).toList)

    val dfWithInputColumns = spark
      .read
      .schema(inputColumnNameSchema)
      .withOptions
      .xml(path)

    inputColumnsMap.foldLeft(dfWithInputColumns) { case (df, (schemaName, inputColumnName)) =>
      if (inputColumnName != schemaName) {
        df.withColumnRenamed(inputColumnName, schemaName)
      } else {
        df
      }
    }
  }

  private def toPowerCenterName(columnName: String): String = {
    val colLC = columnName.toLowerCase
    if (colLC.length > 32) {
      colLC.substring(0, 32)
    } else {
      colLC
    }
  }

  private def readColumnMapping(path: String, spark: SparkSession): Map[String, String] = {
    logger.debug(s"Input: XMLInput read column mapping names from $path")

    import spark.implicits._

    spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true").load(path)
      .map(r => (r(0).toString, r(1).toString))
      .collect()
      .toMap
  }

}
