package com.datio.kirby.output

import com.datio.kirby.config.OutputFactory
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature, TestEntityForPartition}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrameWriter, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class JdbcOutputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with FileTestUtil with OutputFactory {

  private val config = ConfigFactory.parseString(
    s"""
       | output {
       |    type = "jdbc"
       |    url = "jdbc:h2:~/test"
       |    user = "sa"
       |    password = ""
       |    driver = "org.h2.Driver"
       |    table = "testtable"
       |    mode = "append"
       |  }
        """.stripMargin)

  feature("Testing JDBC output") {

    scenario("Write in connector") {

      Given("A configuration")
      val jdbcConfig = config

      import spark.implicits._
      val df = List(TestEntityForPartition("hello", "1"), TestEntityForPartition("world", "2"),
        TestEntityForPartition("!!", "3")).toDF

      When("Run a writer in path")
      val JdbcOutputMock = new JdbcOutput(jdbcConfig.getConfig("output")) {
        override protected def writeDF(dfw: DataFrameWriter[Row]): Unit = {
          this.url shouldBe url
          this.table shouldBe table
          this.props.size() shouldEqual 3
        }
      }

      Then("Write should run normally")
      JdbcOutputMock.write(df)
    }

    scenario("Check malformed url") {

      Given("A configuration malformed")
      val jdbcConfig = config
      import spark.implicits._
      val df = List("foo", "bar").toDF()

      When("Run a writer in path should fail")
      an[ClassNotFoundException] should be thrownBy readOutput(jdbcConfig.getConfig("output")).write(df)

    }
  }
}
