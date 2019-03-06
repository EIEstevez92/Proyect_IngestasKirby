package com.datio.kirby.output

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.datio.kirby.testUtils._
import org.apache.spark.sql.DataFrame

@RunWith(classOf[JUnitRunner])
class CsvOutputTest extends OutputTest {
  override val outputType = "csv"

  override def readData(path: String): DataFrame = spark.read.schema(schemaTestEntityForPartition).csv(path)

}