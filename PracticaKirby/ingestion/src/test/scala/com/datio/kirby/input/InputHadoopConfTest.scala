package com.datio.kirby.input

import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class InputHadoopConfTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  feature("check add hadoop configuration when a input need it, for example: access key, secret key for S3 files") {

    scenario("read config file without hadoop configuration") {
      testCase = "CD-2"
      Given("path to file")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "csv"
          |    paths = ["src/test/resources/configFiles/csv/simple_example.csv"]
          |    delimiter = ";"
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(inputConfig.getConfig("input")) {
        override lazy val schema = StructType(Array(StructField("cod_entalfa", StringType)))
      }
      reader.read(spark)

      Then("SparkContext.hadoopConfiguration no contain a configuration")
      spark.sparkContext.hadoopConfiguration.get("fs.s3a.access.key") shouldBe None.orNull
    }

    scenario("read config file with hadoop configuration") {
      testCase = "CD-1"
      Given("path to file")

      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "csv"
          |    hadoop.fs.s3a.access.key ="XXXX"
          |    kirby.input.hadoop.fs.s3a.secret.key = "YYYYYYYYYZZZZZZZZZZZ"
          |    paths = ["src/test/resources/configFiles/csv/simple_example.csv"]
          |    delimiter = ";"
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(inputConfig.getConfig("input")) {
        override lazy val schema = StructType(Array(StructField("cod_entalfa", StringType)))
      }
      reader.read(spark)

      Then("SparkContext.hadoopConfiguration contain a configuration")
      spark.sparkContext.hadoopConfiguration.get("fs.s3a.access.key") shouldBe "XXXX"
    }

  }
}
