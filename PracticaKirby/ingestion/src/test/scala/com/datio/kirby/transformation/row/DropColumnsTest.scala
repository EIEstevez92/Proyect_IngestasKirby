package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFunSuite
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class DropColumnsTest extends InitSparkSessionFunSuite with TransformationFactory {

  test("Drop column correctly") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropColumns"
        |  columnsToDrop : ["dropColumn(strange)"]
        |}
      """.stripMargin)

    import spark.implicits._

    val df: DataFrame = List(("aa", "") ,("bb", ""),("cc", "")).toDF("text","dropColumn(strange)")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.columns.length == 1)

  }

  test("Dont drop any column if not exist column to drop") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropColumns"
        |  columnsToDrop : ["unknownColumn"]
        |}
      """.stripMargin)

    import spark.implicits._

    lazy val df: DataFrame = List(("dd", "") ,("ee", ""),("ff", "")).toDF("text","dropColumn")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.columns.length == 2)
  }

  test("Dont drop any column if array is empty") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "dropColumns"
        |  columnsToDrop : []
        |}
      """.stripMargin)

    import spark.implicits._

    lazy val df: DataFrame = List(("gg", "") ,("hh", ""),("11", "")).toDF("text","dropColumn")

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.columns.length == 2)

  }

}