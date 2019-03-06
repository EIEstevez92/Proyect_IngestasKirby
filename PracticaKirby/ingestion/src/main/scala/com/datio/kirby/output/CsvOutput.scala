package com.datio.kirby.output

import com.datio.kirby.api.Output
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrameWriter, Row}

@configurable("csv")
class CsvOutput(val config: Config) extends Output {

  override def writeDF(dfw: DataFrameWriter[Row]): Unit = {

    logger.info("Output: CsvOutput writer")

    dfw
      .withOptions
      .csv(path)
  }
}
