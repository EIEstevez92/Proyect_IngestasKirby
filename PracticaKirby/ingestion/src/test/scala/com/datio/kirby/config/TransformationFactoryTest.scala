package com.datio.kirby.config

import com.datio.kirby.CheckFlow
import com.datio.kirby.testUtils.InitSparkSessionFunSuite
import com.datio.kirby.transformation.column.{Literal, Replace}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TransformationFactoryTest extends InitSparkSessionFunSuite with CheckFlow with TransformationFactory {

  val metadataPK: Metadata = {
    new MetadataBuilder()


  }.putBoolean("pk", value = true)
    .build()

  test("When config comes with dropduplicates, replace, trim and notnull") {
    val config = ConfigFactory.parseString(
      """
        |transformations =
        |[
        |  {
        |    field = "text"
        |    type = "trim"
        |    trimType = "both"
        |  },
        |  {
        |    type : "cleanNulls"
        |  },
        |  {
        |    field = "text"
        |    type = "replace"
        |    replace = {
        |      "dd" : "aa",
        |      "bb" : "aa",
        |      "cc" : "aa",
        |    }
        |  },
        |  {
        |    type : "dropDuplicates"
        |  }
        |]
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa") :: Row(" aa  ") :: Row(null) :: Row("  bb  ") :: Row(null) :: Row("  cc  ") ::
      Row(null) :: Row(" dd") :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava,
      StructType(
        StructField("text", StringType, metadata = metadataPK) :: Nil
      )
    )

    val transformations = config.getConfigList("transformations").asScala.map(conf => readTransformation(conf)(spark))

    val dfCleaned = applyTransformations(df, transformations)(spark)

    assert(dfCleaned.count() == 1)

    assert(dfCleaned.first().getAs[String]("text") == "aa")

  }

  test("When config comes with dropduplicates enabled") {
    val config = ConfigFactory.parseString(
      """
        |transformations =
        |[
        |  {
        |    type : "dropDuplicates"
        |  }
        |]
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa") :: Row("aa") :: Row(null) :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava,
      StructType(
        StructField("text", StringType, metadata = metadataPK) :: Nil
      )
    )

    val dfCleaned = config.getConfigList("transformations").asScala
      .foldLeft(df)((dfA, conf) => readTransformation(conf)(spark).transform(dfA))

    assert(dfCleaned.count() == 2)

    val arrayRows = dfCleaned.collect()

    assert(arrayRows.contains(Row("aa")))
    assert(arrayRows.contains(Row(null)))

  }

  test("When config comes with notNull enabled") {
    val config = ConfigFactory.parseString(
      """
        |transformations =
        |[
        |  {
        |    type : "cleanNulls"
        |  }
        |]
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa") :: Row("aa") :: Row(null) :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava,
      StructType(
        StructField("text", StringType, metadata = metadataPK) :: Nil
      )
    )

    val dfCleaned = config.getConfigList("transformations").asScala
      .foldLeft(df)((dfA, conf) => readTransformation(conf)(spark).transform(dfA))

    assert(dfCleaned.count() == 2)

    assert(dfCleaned.collect().head.getAs[String]("text") == "aa")
    assert(dfCleaned.collect().last.getAs[String]("text") == "aa")

  }

  test("When config comes without any cleaner") {
    val config = ConfigFactory.parseString(
      """
        |transformations =
        |[
        |
        |]
      """.stripMargin)

    val columnToCheck: List[Row] = Row("aa") :: Row("aa") :: Row(null) :: Nil

    lazy val df: DataFrame = spark.createDataFrame(columnToCheck.asJava,
      StructType(
        StructField("text", StringType, metadata = metadataPK) :: Nil
      )
    )

    val transformations = config.getConfigList("transformations").asScala.map(conf => readTransformation(conf)(spark))

    val dfCleaned = applyTransformations(df, transformations)(spark)

    assert(dfCleaned.count() == 3)

    assert(dfCleaned.collect().head.getAs[String]("text") == "aa")
    assert(dfCleaned.collect().tail.head.getAs[String]("text") == "aa")
    assert(dfCleaned.collect().last.get(0) == null)

  }

  test("When config comes with regex 1 match") {
    val config = ConfigFactory.parseString(
      """
        |  {
        |    field = ".*FIELD.*"
        |    regex = true
        |    type = "replace"
        |    replace = {
        |      "f1": "f2"
        |    }
        |  }
      """.stripMargin)

    val columns = Array("UnknownField", "test_FIELD_1", "FIELD_2")

    val transformations = expandTransformation(columns, readTransformation(config)(spark))(spark)

    assert(transformations.length == 2)
    assert(transformations.map(_.isInstanceOf[Replace]).reduce(_ && _))
  }

  test("When config comes with regex 2 match") {
    val config = ConfigFactory.parseString(
      """
        |  {
        |    field = "^[A-Z0-9_]*$"
        |    regex = true
        |    type = "literal"
        |    default = "X"
        |    defaultType = "string"
        |  }
      """.stripMargin)

    val columns = Array("UnknownField", "test_FIELD_1", "FIELD_2")

    val transformations = expandTransformation(columns, readTransformation(config)(spark))(spark)

    assert(transformations.length == 1)
    assert(transformations.map(_.isInstanceOf[Literal]).reduce(_ && _))
  }

  test("When config comes with regex none match") {
    val config = ConfigFactory.parseString(
      """
        |  {
        |    field = "NotRegexField"
        |    type = "trim"
        |    trimType = "both"
        |  }
      """.stripMargin)

    val columns = Array("UnknownField", "test_FIELD_1", "FIELD_2")

    val transformations = expandTransformation(columns, readTransformation(config)(spark))(spark)

    assert(transformations.isEmpty)
  }

}
