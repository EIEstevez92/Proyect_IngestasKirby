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
class RenameColumnsTest extends InitSparkSessionFunSuite with TransformationFactory {

  val schema = StructType(
    StructField("text", StringType) ::
      StructField("text2", StringType) :: Nil
  )

  test("rename all columns correctly") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "renamecolumns"
        |  columnsToRename : {
        |     "text" : "text_renamed",
        |     "text2" : "text_renamed2"
        |  }
        |}
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa", "") :: Row("bb", "") :: Row("cc", "") :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava, schema)

    val dfRenamed = readTransformation(config)(spark).transform(df)

    assert(dfRenamed.count() === 3)
    assert(dfRenamed.columns.length === 2)

    assert(dfRenamed.schema.fields.exists(_.name === "text_renamed"))
    assert(dfRenamed.schema.fields.exists(_.name === "text_renamed2"))

  }

  test("rename some columns correctly") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "renamecolumns"
        |  columnsToRename : {
        |     "text" : "text_renamed",
        |  }
        |}
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa", "") :: Row("bb", "") :: Row("cc", "") :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava, schema)

    val dfRenamed = readTransformation(config)(spark).transform(df)

    assert(dfRenamed.count() === 3)
    assert(dfRenamed.columns.length === 2)

    assert(dfRenamed.schema.fields.exists(_.name === "text_renamed"))
    assert(dfRenamed.schema.fields.exists(_.name === "text2"))

  }

  test("dont rename columns") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "renamecolumns"
        |  columnsToRename : {
        |  }
        |}
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa", "") :: Row("bb", "") :: Row("cc", "") :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava, schema)

    val dfRenamed = readTransformation(config)(spark).transform(df)

    assert(dfRenamed.count() === 3)
    assert(dfRenamed.columns.length === 2)

    assert(dfRenamed.schema.fields.exists(_.name === "text"))
    assert(dfRenamed.schema.fields.exists(_.name === "text2"))

  }


  test("rename column with parenthesis correctly") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "renamecolumns"
        |  columnsToRename : {
        |     "sum(column)" : "sum_column",
        |  }
        |}
      """.stripMargin)
    
    import spark.implicits._
    val df = List(("aa", 1), ("bb", 34), ("cc", 12)).toDF("col1","sum(column)")

    val dfRenamed = readTransformation(config)(spark).transform(df)

    assert(dfRenamed.count() === 3)
    assert(dfRenamed.columns.length === 2)

    assert(dfRenamed.schema.fields.exists(_.name === "col1"))
    assert(dfRenamed.schema.fields.exists(_.name === "sum_column"))

  }
}