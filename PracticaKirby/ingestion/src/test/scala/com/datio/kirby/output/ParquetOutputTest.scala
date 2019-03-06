package com.datio.kirby.output

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParquetOutputTest extends OutputTest {
  override val outputType = "parquet"

  override def readData(path: String) = spark.read.parquet(path)
}

