package com.datio.kirby.input

import com.databricks.spark.avro.AvroDataFrameReader
import com.datio.kirby.CheckFlow
import com.datio.kirby.api.implicits.{ApplyFormat, ApplyRename, EvolutionTypes}
import com.datio.kirby.api.util.FileUtils
import com.datio.kirby.config.{FileReader, configurable}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.util.{Failure, Success, Try}

/**
  * This Class reads a file in avro format and it return a data frame.
  *
  * @param config config for avro input reader.
  */
@configurable("avro")
class AvroInput(val config: Config) extends InputSchemable with ApplyFormat with ApplyRename with EvolutionTypes with FileReader with FileUtils with CheckFlow {


  override lazy val schemaConfig: Config = Try(config.getConfig("schema")).getOrElse(ConfigFactory.empty())

  override lazy val schema: StructType = readSchema(schemaConfig, mandatory = false, includeDeleted = true, includeMetadata = true)

  lazy val applyConversions: Boolean = Try(config.getBoolean("applyConversions")).getOrElse(true)

  override def reader(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    logger.info("Input: AvroInput reader")

    val partitionColumns = getPartitions(path)

    applySparkOptions(Map("spark.sql.sources.partitionColumnTypeInference.enabled" -> false))(spark)
    val df = spark
      .read
      .withOptions
      .withAvroSchema(partitionColumns)
      .avro(path)

    if (applyConversions) {
      df.resolveEvolution
        .castSchemaFormat(StructType(schema.filter(c => partitionColumns.contains(c.name))))
        .castOriginTypeDate(schema)
        .castOriginTypeDecimal(schema)
        .castOriginTypeTimestamp(schema)
        .renameColumns(schema)
    } else {
      df
    }
  }

  protected implicit class AvroInputSchema(dfReader: DataFrameReader) {

    import org.json4s._

    def withAvroSchema(partitionColumns: Seq[String]): DataFrameReader = {
      Try(config.getString("schema.path")) match {
        case Failure(_: ConfigException.Missing) => dfReader
        case Success(path) =>
          val schema = parseAvroSchema(path, partitionColumns)
          dfReader.option("avroSchema", schema.toString)
        case Failure(e)
        => throw new RuntimeException(s"AvroInput: error getting schema path from configuration: $config", e)
      }
    }

    protected def parseAvroSchema(path: String, partitionColumns: Seq[String]): Schema = {
      val avroSchema = readJson(path)
      val jsonSchemaContent: JValue = removePartitionColumns(avroSchema, partitionColumns)
      val str = compact(render(jsonSchemaContent))
      logger.debug(s"AvroInput: avroSchema parsed =>$str")
      new Schema.Parser().parse(str)
    }

    protected def removePartitionColumns(avroSchema: JValue, partitionColumns: Seq[String]): JValue = {
      avroSchema.transformField {
        case ("fields", fields: JArray) =>
          val newFields = partitionColumns.foldLeft(fields)((f, c) => f.remove(field => field \ "name" == JString(c)).asInstanceOf[JArray])
          logger.info(s"AvroInput: removed partition columns (${partitionColumns.mkString(", ")}) " +
            s"from schema. Final fields: ${newFields.children.map(f => (f \ "name").values).mkString(", ")}")
          ("fields", newFields)
      }
    }
  }

}