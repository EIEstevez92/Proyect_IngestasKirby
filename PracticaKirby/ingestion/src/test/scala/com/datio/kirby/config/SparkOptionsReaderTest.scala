package com.datio.kirby.config

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SPARK_OPTION_WRONG_TYPE
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkOptionsReaderTest extends FeatureSpec with GivenWhenThen with Matchers with SparkOptionReader {

  val RUNNING_APP: String = "Run the app"
  var spark: SparkSession = _

  val TEST_SPARK_OPTIONS = Seq(
    BooleanSparkOption("sparkOp1", "input.inferTypes"),
    StringSparkOption("sparkOp2", "output.output"),
    LongSparkOption("sparkOp3", "number")
  )

  feature("Errors in the files config") {
    scenario("Invalid spark options") {
      Given("A bad type")
      val badConfig = ConfigFactory.parseString(
        """{
          |  input {
          |    inferTypes = "tru"
          |  }
          |}""".stripMargin)

      When("Load spark options")
      val kException: Exception = intercept[KirbyException] {
        readSparkOptions(badConfig, TEST_SPARK_OPTIONS)
      }

      Then("input var paths does not exist")
      assert(kException.getMessage == SPARK_OPTION_WRONG_TYPE.toFormattedString("input.inferTypes"))
    }
  }

  feature("Valid Spark Options") {

    scenario("Config File is empty") {
      Given("A good complete configuration")
      val goodConfig = ConfigFactory.parseString(
        """{
          |}
          |""".stripMargin)

      When("Load spark options")
      val opts = readSparkOptions(goodConfig, TEST_SPARK_OPTIONS)

      Then("Result is correct")
      assert(opts.isEmpty)
    }

    scenario("Config File is Right") {
      Given("A good complete configuration")
      val goodConfig = ConfigFactory.parseString(
        """{
          |  input = {
          |    inferTypes = true
          |  }
          |  output = {
          |    output = salida
          |  }
          |  number = 4
          |}
          |""".stripMargin)

      When("Load spark options")
      val opts = readSparkOptions(goodConfig, TEST_SPARK_OPTIONS)

      Then("Result is correct")
      assert((opts.toSet diff Map("sparkOp1" -> true, "sparkOp2" -> "salida", "sparkOp3" -> 4L).toSet).isEmpty)
    }
  }

}