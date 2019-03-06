package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFunSuite
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SelectColumnsTest extends InitSparkSessionFunSuite with TransformationFactory {


  test("SelectColumns select only indicated columns") {
    import spark.implicits._

    val df: DataFrame = List(
      ("row11", "row12", "row13", "row14", "row15"),
      ("row21", "row22", "row23", "row24", "row25"))
      .toDF("column1", "column2", "column3", "column4", "column(5)")

    val columnsToSelect = Seq("column2", "column3", "column(5)")

    val config = ConfigFactory.parseString(
      s"""
         |{
         |  type : "selectColumns"
         |  columnsToSelect : [${columnsToSelect.mkString(", ")}]
         |}
      """.stripMargin)

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.columns.length == columnsToSelect.size)
    assert(columnsToSelect.forall(dfCleaned.columns.contains(_)))
  }

  test("SelectColumns throws error when select zero columns") {
    import spark.implicits._

    val df: DataFrame = List(
      ("row11", "row12", "row13", "row14", "row15"),
      ("row21", "row22", "row23", "row24", "row25"))
      .toDF("column1", "column2", "column3", "column4", "column(5)")

    val config = ConfigFactory.parseString(
      s"""
         |{
         |  type : "selectColumns"
         |  columnsToSelect : [ ]
         |}
      """.stripMargin)

    assertThrows[SelectColumnsException](readTransformation(config)(spark).transform(df))
  }

  test("SelectColumns does not select any column that does not exist and throws error if you try") {

    import spark.implicits._

    val df: DataFrame = List(
      ("row11", "row12", "row13", "row14", "row15"),
      ("row21", "row22", "row23", "row24", "row25"))
      .toDF("column1", "column2", "column3", "column4", "column(5)")

    val columnsToSelect = Seq("notExist")

    val config = ConfigFactory.parseString(
      s"""
         |{
         |  type : "selectColumns"
         |  columnsToSelect : [${columnsToSelect.mkString(", ")}]
         |}
      """.stripMargin)

    assertThrows[AnalysisException](readTransformation(config)(spark).transform(df))
  }

}
