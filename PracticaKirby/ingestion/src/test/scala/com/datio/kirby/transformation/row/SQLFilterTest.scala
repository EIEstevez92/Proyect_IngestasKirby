package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.{InitSparkSessionFeature, _}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class SQLFilterTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Filter Data") {
    scenario("check correct result with complex filter") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        type = "sqlFilter"
           |        filter = "weight > 40 AND (name = 'juan' OR name = 'samuel')"
           |      }
        """.stripMargin
      )

      Given("input dataFrame")

      import spark.implicits._
      val df = List(FilterByFieldTestUser("juan", 30),
        FilterByFieldTestUser("ruben", 12),
        FilterByFieldTestUser("juan", 60),
        FilterByFieldTestUser("samuel", 103)).toDF

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("Result must be correct")

      dfResult.count shouldBe 2
    }

    scenario("check malformed filter with invalid column names") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        type = "sqlFilter"
           |        filter = "weit > 40 AND (name = 'juan' OR name = 'samuel')"
           |      }
        """.stripMargin
      )

      Given("input dataFrame")

      import spark.implicits._
      val df = List(FilterByFieldTestUser("juan", 30),
        FilterByFieldTestUser("ruben", 12),
        FilterByFieldTestUser("juan", 60),
        FilterByFieldTestUser("samuel", 103)).toDF

      When("apply transformations")
      Then("Result must be correct")
      intercept[AnalysisException] {
        readTransformation(config)(spark).transform(df)
      }
    }

    scenario("check malformed filter with invalid operator") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        type = "sqlFilter"
           |        filter = "weight <!> 40 AND (name = 'juan' OR name = 'samuel')"
           |      }
        """.stripMargin
      )

      Given("input dataFrame")

      import spark.implicits._
      val df = List(FilterByFieldTestUser("juan", 30),
        FilterByFieldTestUser("ruben", 12),
        FilterByFieldTestUser("juan", 60),
        FilterByFieldTestUser("samuel", 103)).toDF

      When("apply transformations")
      Then("Result must be correct")
      intercept[ParseException] {
        readTransformation(config)(spark).transform(df)
      }
    }
  }

  scenario("check filter with not allowed operators") {
    Given("config")

    val config = ConfigFactory.parseString(
      s"""
         |      {
         |        type = "sqlFilter"
         |        filter = "weight > 40 AND (name = 'juan' OR name = 'samuel') GROUP BY weight"
         |      }
        """.stripMargin
    )

    Given("input dataFrame")

    import spark.implicits._
    val df = List(FilterByFieldTestUser("juan", 30),
      FilterByFieldTestUser("ruben", 12),
      FilterByFieldTestUser("juan", 60),
      FilterByFieldTestUser("samuel", 103)).toDF

    When("apply transformations")
    Then("Result must be correct")
    intercept[ParseException] {
      readTransformation(config)(spark).transform(df)
    }
  }

}
