package com.datio.kirby

import com.datio.kirby.config.OutputFactory
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class LauncherTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with FileTestUtil with OutputFactory {


  feature("Testing Launcher") {

    scenario("Generate business info correctly") {

      Given("A configuration")

      val myConfig = ConfigFactory.parseString(
        s"""
           |kirby{
           |  output {
           |    path = "hdfs://hadoop:9000/tests/flow/csv/transformation_data.csv"
           |    reprocess = [ "date=today" ]
           |    mode = "reprocess"
           |    schema {
           |      path = "hdfs://hadoop:9000/tests/flow/schema/avro/schema_transformation.csv"
           |    }
           |  }
           |  input {}
           |  transformations {}
           |}
        """.stripMargin)

      When("Run a writer in path")
      val bi = Launcher.defineBusinessInfo(myConfig)
      Then("")
      bi.exitCode shouldBe 0
      bi.entity shouldBe "schema_transformation"
      bi.path shouldBe "hdfs://hadoop:9000/tests/flow/csv/transformation_data.csv"
      bi.mode shouldBe "reprocess"
      bi.schema shouldBe "hdfs://hadoop:9000/tests/flow/schema/avro/schema_transformation.csv"
      bi.schemaVersion shouldBe "1.0"
      bi.reprocessing shouldBe "date=today"

    }

  }

}