package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFunSuite
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CleanNullsTest extends InitSparkSessionFunSuite with TransformationFactory {

  val metadataPK: Metadata = {
    new MetadataBuilder()


  }.putBoolean("pk", value = true)
    .build()

  test("CleanNullsCheck only when field is null") {

    testCase = "CD-36"

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "cleanNulls"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String]("aa", "aa", null).toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)


    assert(dfCleaned.count() == 2)

    assert(dfCleaned.first().getAs[String]("text") == "aa")
    assert(dfCleaned.collect().last.getAs[String]("text") == "aa")

    result = true
  }

  test("CleanNullsCheck eliminate all duplicates") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "cleannulls"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String](null, null, null).toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 0)

  }

  test("CleanNullsCheck dont eliminate if has not nulls") {
    testCase = "CD-37"

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "cleannulls"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List[String]("aa", "bb", "cc").toDF("text")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)

    assert(dfCleaned.first().getAs[String]("text") == "aa")
    assert(dfCleaned.collect().tail.head.getAs[String]("text") == "bb")
    assert(dfCleaned.collect().last.getAs[String]("text") == "cc")

    result = true
  }

  test("CleanNullsCheck only for primary keys") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "cleannulls"
        |  primaryKey : ["text"]
        |}
      """.stripMargin)

    import spark.implicits._
    val df: DataFrame = List((null, "aa"), ("bb", null), ("cc", "cc")).toDF("text", "text2")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 2)

    assert(dfCleaned.collect().map(e => (e.getAs[String]("text"), e.getAs[String]("text2"))).toSet === Set(("bb", null), ("cc", "cc")))
  }

  test("CleanNullsCheck for all columns if never of them has primary keys") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "cleannulls"
        |}
      """.stripMargin)


    import spark.implicits._
    val df: DataFrame = List((null, "aa"), ("bb", null), ("cc", "cc")).toDF("text", "text2")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 1)

    assert(dfCleaned.collect().map(e => (e.getAs[String]("text"), e.getAs[String]("text2"))).toSet === Set(("cc", "cc")))

  }

}
