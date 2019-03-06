package com.datio.kirby.output

import com.datio.kirby.api.Output
import com.datio.kirby.api.exceptions.KirbyException
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrameWriter, Row}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OutputTraitTest extends FunSuite with Output {

  override protected def writeDF(dfw: DataFrameWriter[Row]) = {}

  override protected implicit val config: Config = ConfigFactory.empty()

  override lazy val finalPath = ""

  test("isPartition table") {
    assert(isPartition("catalog.parquet") === false, "table")
  }

  test("isPartition hdfs table") {
    assert(isPartition("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet") === false, "hdfs table")
  }

  test("isPartition one partition") {
    assert(isPartition("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet/catalog=126") === true, "one partition")
  }

  test("isPartition two partitions") {
    assert(isPartition("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet/catalog=126/date=2017-12-04") === true, "two partitions")
  }

  test("isPartition table inside partition") {
    assert(isPartition("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet/catalog=126/tabla.avro") === false, "table inside partition")
  }

  test("isPartition empty partition") {
    assert(isPartition("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet/catalog=") === true, "empty partition")
  }

  test("checkReprocessPartition") {
    assertThrows[KirbyException](checkReprocessPartition(List("hdfs://hadoop:9000/tests/flow/result/parquet/catalog.parquet/catalog="), List("catalog")))

  }

  test("checkReprocessPartition one partition") {
    checkReprocessPartition(List("catalog=first"), List("catalog"))
    checkReprocessPartition(List("fec_ini_proc=2009-07-03"), List("fec_ini_proc", "fec_cierre"))
  }

  test("checkReprocessPartition one partition and reprocess empty") {
    checkReprocessPartition(List("catalog="), List("catalog"))
  }

  test("checkReprocessPartition three partition and two reprocess") {
    checkReprocessPartition(List("catalog=/book=first"), List("catalog", "book", "text"))
  }

  test("checkReprocessPartition three partition and three reprocess") {
    checkReprocessPartition(List("catalog=first/book=first/text=first"), List("catalog", "book", "text"))
  }

  test("checkReprocessPartition with not exist reprocess partition") {
    assertThrows[KirbyException](checkReprocessPartition(List("book=first"), List("catalog", "book", "text")))
  }

  test("checkReprocessPartition with not exist reprocess empty partition") {
    assertThrows[KirbyException](checkReprocessPartition(List("book="), List("catalog", "book", "text")))
  }

  test("checkReprocessPartition with not exist reprocess partition and other partition") {
    assertThrows[KirbyException](checkReprocessPartition(List("book=first/text=text"), List("catalog", "book", "text")))
    assertThrows[KirbyException](checkReprocessPartition(List("text=first/book=text"), List("catalog", "book", "text")))
    assertThrows[KirbyException](checkReprocessPartition(List("catalog=first/book=first/not=first"), List("catalog", "book", "text")))
    assertThrows[KirbyException](checkReprocessPartition(List("catalog=first/book=first/text=first/not=first"), List("catalog", "book", "text")))
  }

}
