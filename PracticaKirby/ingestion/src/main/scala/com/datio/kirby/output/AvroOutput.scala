package com.datio.kirby.output

import com.databricks.spark.avro.AvroDataFrameWriter
import com.datio.kirby.api.Output
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrameWriter, Row}

/**
  * This class saves a data frame in avro format.
  *
  * @param config config for avro output writer.
  */
@configurable("avro")
class AvroOutput(val config: Config) extends Output {

  override def writeDF(dfw: DataFrameWriter[Row]): Unit = {

    logger.info("Output: AvroOutput writer")

    dfw
      .withOptions
      .avro(path)
  }
}
