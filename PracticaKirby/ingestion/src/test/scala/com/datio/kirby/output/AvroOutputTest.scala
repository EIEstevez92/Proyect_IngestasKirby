package com.datio.kirby.output

import com.databricks.spark.avro.AvroDataFrameReader
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroOutputTest extends OutputTest {
  override val outputType = "avro"

  override def readData(path: String) = spark.read.avro(path)
}