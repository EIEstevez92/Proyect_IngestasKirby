package com.datio.kirby.input

import com.datio.kirby.api.Input
import com.datio.kirby.config.SchemaReader
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * This Trait add a schema to the input trait
  *
  * Mandatory parameters:
  * - paths: Array with files you want to read.
  * - schema.path: schema file
  *
  */
trait InputSchemable extends Input with SchemaReader {

  lazy val schemaConfig: Config = Try(config.getConfig("schema"))
    .getOrElse(throw new RuntimeException(s"InputSchemable: schema is mandatory. ${config.toString}"))

  lazy val schema: StructType = readSchema(schemaConfig)

  override def reader(spark: SparkSession)(path: String): DataFrame = {
    logger.debug("InputSchemable: Schema read => {} ", schema.fields.map(field => s"${field.name} -> ${field.dataType.toString}"))
    reader(spark, path, schema)
  }

  def reader(spark: SparkSession, path: String, schema: StructType): DataFrame
}
