package com.datio.kirby.constants

import com.datio.kirby.config.BooleanSparkOption
import org.apache.spark.sql.internal.SQLConf


object ConfigConstants {

  val APPLICATION_CONF_SCHEMA = "kirby.schema.json"

  val ROOT_CONF_KEY = "kirby"
  val TRANSFORMATION_CONF_KEY = "transformations"
  val SCHEMA_CONF_KEY = "schema"
  val INPUT_CONF_KEY = "input"
  val OUTPUT_CONF_KEY = "output"

  val SCHEMA_CONSTANT = "schema"
  val INPUT_OPTIONS_SUBCONFIG = "options"

  val SPARK_OPTIONS = Seq(
    BooleanSparkOption(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE.key, "input.inferTypes")
  )

}