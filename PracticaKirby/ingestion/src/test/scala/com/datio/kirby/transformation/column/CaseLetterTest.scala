package com.datio.kirby.transformation.column

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class CaseLetterTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Lower and Upper case transformation") {
    scenario("upper case") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |  {
          |    type = "caseletter"
          |    field = "text"
          |    operation = "upper"
          |  }
          |  """.stripMargin)

      Given("column to uppercase")
      import spark.implicits._
      val df = List("aa", "bB", "").toDF("text")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be in uppercase")
      dfResult.select("text").collect.map(r => r.getAs[String]("text")).toSet shouldBe Set("AA", "BB", "")
    }

    scenario("lower case") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |  {
          |    type = "caseletter"
          |    field = "text"
          |    operation = "lower"
          |  }
          |  """.stripMargin)

      Given("column to uppercase")
      import spark.implicits._
      val df = List("aa", "bB", "").toDF("text")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be in lowercase")
      dfResult.select("text").collect.map(r => r.getAs[String]("text")).toSet shouldBe Set("aa", "bb", "")
    }
  }

  scenario("configuration error") {
    Given("config")

    val config = ConfigFactory.parseString(
      """
        |  {
        |    type = "caseletter"
        |    field = "text"
        |  }
        |  """.stripMargin)

    Given("column to uppercase")
    import spark.implicits._
    val df = List("aa", "bB", "").toDF("text")

    When("apply transformations")
    val e = intercept[KirbyException] {
      readTransformation(config)(spark).transform(df)
    }
    Then("text should be in uppercase")
    e.getMessage shouldBe "CaseLetter transformation: 'operation' attribute is mandatory (allowed values are 'upper' and 'lower')"
  }
}