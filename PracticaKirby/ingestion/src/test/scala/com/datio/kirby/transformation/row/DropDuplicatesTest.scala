package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFunSuite
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DropDuplicatesTest extends InitSparkSessionFunSuite with TransformationFactory {

  val metadataPK: Metadata = {
    new MetadataBuilder()
  }.putBoolean("pk", value = true).build()

  test("DropDuplicatesCheck eliminate only duplicates") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropDuplicates"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String]("aa", "aa", null).toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 2)

    val arrayRows = dfCleaned.collect()

    assert(arrayRows.contains(Row(null)))
    assert(arrayRows.contains(Row("aa")))
  }

  test("DropDuplicatesCheck eliminate all duplicates") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropDuplicates"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String]("aa", "aa", "aa").toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 1)

    assert(dfCleaned.first().getAs[String]("text") == "aa")

  }

  test("DropDuplicatesCheck dont eliminate if has not duplicates") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropDuplicates"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String]("aa", "bb", "cc").toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)

    val arrayRows = dfCleaned.collect()

    assert(arrayRows.contains(Row("aa")))
    assert(arrayRows.contains(Row("bb")))
    assert(arrayRows.contains(Row("cc")))

  }

  test("DropDuplicatesCheck only for primary keys") {
    testCase = "CD-38"
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropduplicates"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List(("aa", "aa"), ("aa", "cc"), ("cc", "aa")).toDF("text", "text2")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 2)

    val arrayRows = dfCleaned.collect()

    assert(arrayRows.contains(Row("aa", "aa")))
    assert(arrayRows.contains(Row("cc", "aa")))

    result = true
  }

  test("DropDuplicatesCheck for all columns if never of them has primary keys") {
    testCase = "CD-39"

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropduplicates"
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List(("bb", "aa"), ("dd", "cc"), ("bb", "cc")).toDF("text", "text2")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)

    val arrayRows = dfCleaned.collect()

    assert(arrayRows.contains(Row("bb", "aa")))
    assert(arrayRows.contains(Row("dd", "cc")))
    assert(arrayRows.contains(Row("bb", "cc")))

    result = true
  }

}
