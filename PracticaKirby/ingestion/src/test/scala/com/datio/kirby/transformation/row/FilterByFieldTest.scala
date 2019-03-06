package com.datio.kirby.transformation.row

import java.sql.Date
import java.util.Calendar

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.{InitSparkSessionFunSuite, _}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterByFieldTest extends InitSparkSessionFunSuite with TransformationFactory {

  val caseTestWithoutNulls = List(FilterByFieldTestUser("Antonio", 12), FilterByFieldTestUser("Samuel", 1),
    FilterByFieldTestUser("Maria", 1), FilterByFieldTestUser("Alvaro", 2), FilterByFieldTestUser("Antonio", 3))

  val caseTestWithNulls = List(FilterByFieldTestUser("Antonio", 12), FilterByFieldTestUser("Samuel", 1),
    FilterByFieldTestUser("Maria", 1), FilterByFieldTestUser(null, 2), FilterByFieldTestUser("Antonio", 3))


  test("FilterByFieldCheck try to filter when value is Antonio") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name(name)"
        |   op : "eq"
        |  }]
        |}
      """.stripMargin)


    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF("name(name)","weight")

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 2)
    assert(dfCleaned.first().getAs[String]("name(name)") == "Antonio")
  }

  test("FilterByFieldCheck try to filter when value is Maria") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Maria"
        |   field : "name"
        |   op : "eq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 1)
    assert(dfCleaned.first().getAs[String]("name") == "Maria")
  }

  test("FilterByFieldCheck try to filter when value is Maria with nulls") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Maria"
        |   field : "name"
        |   op : "eq"
        |  }]
        |}
      """.stripMargin)


    import spark.implicits._
    val dfwithNulls = caseTestWithNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithNulls)

    assert(dfCleaned.count() == 1)
    assert(dfCleaned.first().getAs[String]("name") == "Maria")
  }

  test("FilterByFieldCheck try to filter when value is Mariano") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Mariano"
        |   field : "name"
        |   op : "eq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithNulls = caseTestWithNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithNulls)

    assert(dfCleaned.count() == 0)
  }

  test("FilterByFieldCheck try to filter when value is Antonio for not equals") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "neq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.first().getAs[String]("name") == "Samuel")

  }

  test("FilterByFieldCheck try to filter when value is Antonio for lt") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "lt"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 1)
    assert(dfCleaned.first().getAs[String]("name") == "Alvaro")

  }

  test("FilterByFieldCheck try to filter when value is Antonio for leq") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "leq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.first().getAs[String]("name") == "Antonio")

  }

  test("FilterByFieldCheck try to filter when value is Antonio for gt") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "gt"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 2)
    assert(dfCleaned.first().getAs[String]("name") == "Samuel")

  }

  test("FilterByFieldCheck try to filter when value is Antonio for geq") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "geq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 4)
    assert(dfCleaned.first().getAs[String]("name") == "Antonio")
  }


  test("FilterByFieldCheck try to filter when value is Antonio for like") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "A%o"
        |   field : "name"
        |   op : "like"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.first().getAs[String]("name") == "Antonio")
  }

  test("FilterByFieldCheck try to filter when value is Antonio for rlike") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "A.*o"
        |   field : "name"
        |   op : "rlike"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 3)
    assert(dfCleaned.first().getAs[String]("name") == "Antonio")
  }


  test("FilterByFieldCheck try to filter when value is Antonio for an unsupported op") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "wololoo"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = intercept[Exception] {
      readTransformation(config)(spark).transform(dfwithoutNulls)
    }
    assert(dfCleaned.getMessage == "[FilterByField] 'op' is required")
  }

  test("FilterByFieldCheck try with several values") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "neq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    val config2 = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "Samuel"
        |   field : "name"
        |   op : "neq"
        |  }]
        |}
      """.stripMargin)

    val dfCleaned2 = readTransformation(config2)(spark).transform(dfwithoutNulls)

    assert(dfCleaned2.count == 4)
  }

  test("FilterByFieldCheck with some filters AND") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  logicOp : and
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "neq"
        |  },
        |  {
        |   value : "Samuel"
        |   field : "name"
        |   op : "neq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 2)
  }

  test("FilterByFieldCheck with some filters OR") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  logicOp : or
        |  filters: [{
        |   value : "Antonio"
        |   field : "name"
        |   op : "eq"
        |  },
        |  {
        |   value : "Samuel"
        |   field : "name"
        |   op : "eq"
        |  }]
        |}
      """.stripMargin)

    import spark.implicits._
    val dfwithoutNulls = caseTestWithoutNulls.toDF

    val dfCleaned = readTransformation(config)(spark).transform(dfwithoutNulls)

    assert(dfCleaned.count() == 3)
  }

  test("FilterByFieldCheck try to filter when value is 2017-07-14") {

    val config = ConfigFactory.parseString(
      """
        |{
        |  type : "filter"
        |  filters: [{
        |   value : "2000-07-14"
        |   field : "date"
        |   op : "gt"
        |  }]
        |}
      """.stripMargin)

    import scala.collection.JavaConverters._

    val calendar: Calendar = Calendar.getInstance()
    val dateToTest: Date = new Date(calendar.getTime.getTime)
    val columnToParse: List[Row] = Row(dateToTest) :: Row(dateToTest) :: Row(dateToTest) :: Nil
    val df = spark.createDataFrame(columnToParse.asJava, StructType(StructField("date", DateType) :: Nil))

    val dfCleaned = readTransformation(config)(spark).transform(df)

    assert(dfCleaned.count() == 3)
  }
}
