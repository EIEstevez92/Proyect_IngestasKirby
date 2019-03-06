package com.datio.kirby.flow

import com.datio.kirby.flow.schema._
import org.apache.spark.sql.types.StructType

object SchemasFlow extends MrrAvroSchema with MrrParquetSchema
  with FixedAvroSchema with PaaAvroSchema with PaaParquetSchema
  with LegacySchema with LegacyParquetSchema with LegacyErrorSchema
  with MrrCsvSchema{

  def getSchema(typeSchema : String, file : String) : StructType = {

    typeSchema match {
      case "avro" => getSchemaAvroFor(file)
      case "parquet" => getSchemaParquetFor(file)

    }
  }

  private def getSchemaCsvFor(file: String) : StructType = {
    file match {
      case "kdat_mrr2" => mrrCsvSchema
      case _ => throw new RuntimeException("Dont match file in schemas flow")
    }
  }

  private def getSchemaAvroFor(file: String) : StructType = {
    file match {
      case "kdat_mrr" => mrrAvroSchema
      case "kdat_paa" => paaAvroSchema
      case "fixed" => fixedAvroSchema
      case "legacy" => legacySchema
      case "legacy_err" => legacyErrorSchema
      case _ => throw new RuntimeException("Dont match file in schemas flow")
    }
  }

  private def getSchemaParquetFor(file: String) : StructType = {
    file match {
      case "kdat_mrr" => mrrParquetSchema
      case "kdat_paa" => paaParquetSchema
      case "legacy" => legacyParquetSchema
      case _ => throw new RuntimeException("Dont match file in schemas flow")
    }
  }
}

