package com.datio.kirby.transformation.row

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RegexColumnTest extends InitSparkSessionFeature  with TransformationFactory with GivenWhenThen with Matchers {

  import spark.implicits._

  feature("Regex Transformation") {
    scenario("Create new column correctly") {

      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "regexcolumn"
          |  regexPattern: "a.?a"
          |  columnToRegex: "text"
          |  regex : [
          |  {
          |    regexGroup: 0
          |    field: "text3"
          |  }
          |  ]
          |}
        """.
          stripMargin)

      Given("A Input Dataframe")
      val df = List(("aaa", ""), ("bbb", ""), ("aca", ""), ("adb", ""), ("aea", "")).toDF("text", "text2")
      When("Perform the regex")
      val dfRenamed = readTransformation(config)(spark).transform(df)
      Then("result number of elements must be 5")
      assert(dfRenamed.count() === 5)
      Then("result number of elements with value must be 3 ")
      assert(dfRenamed.where(dfRenamed.col("text3") =!= "").count() === 3)
    }

    scenario("Create two new columns correctly") {

      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "regexcolumn"
          |  regexPattern: "([a-z]*).([a-z]*)"
          |  columnToRegex: "text"
          |  regex : [
          |  {
          |    regexGroup: 1
          |    field: "text2"
          |  },
          |  {
          |    regexGroup: 2
          |    field: "text3"
          |  }
          |  ]
          |}
        """.
          stripMargin)

      Given("A Input Dataframe")
      val df = List("aaa.bbb", ".bbb", "aaa.", "cdba").toDF("text")
      When("Perform the regex")
      val dfRenamed = readTransformation(config)(spark).transform(df)
      Then("result number of elements must be 4")
      assert(dfRenamed.count() === 4)
      Then("result number of elements with value in one columns should be 3")
      assert(dfRenamed.where(dfRenamed.col("text2") =!= "").count() === 3)
      Then("result number of elements with value in the other columns should be 3")
      assert(dfRenamed.where(dfRenamed.col("text3") =!= "").count() === 2)
    }

    scenario("Create a regex with group number") {

      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "regexcolumn"
          |  regexPattern: "([a-z]*).([a-z]*)"
          |  columnToRegex: "text"
          |  regex : [
          |  {
          |    regexGroup: 1
          |    field: "text5"
          |  }
          |  ]
          |}
        """.
          stripMargin)

      Given("A Input Dataframe")
      val df = List("aaa.bbb", ".bbb", "aaa.", "invalid").toDF("text")
      When("Perform the regex")
      val dfRenamed = readTransformation(config)(spark).transform(df)
      Then("result number of elements must be 4")
      assert(dfRenamed.count() === 4)
      Then("result number of elements with value must be 3")
      assert(dfRenamed.where(dfRenamed.col("text5") =!= "").count() === 3)
    }
  }
}
