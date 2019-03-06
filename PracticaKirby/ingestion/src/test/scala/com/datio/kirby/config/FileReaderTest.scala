package com.datio.kirby.config

import com.datio.kirby.testUtils.InitSparkSessionFeature
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class FileReaderTest extends InitSparkSessionFeature with GivenWhenThen with Matchers {

  feature("parse all lines in file") {
    scenario("check all lines in file are parsed") {
      Given("path to file")
      val reader = new PreMockFileReaderTest()

      val path: String = ClassLoader.getSystemResource("configFiles/fileReader/configSample1.json").getPath

      When("Run config reader in path with one config file")

      val configFile = reader.getContentFromPath(s"file://$path")

      Then("Return config for all fields")
      configFile.length shouldBe 10712

    }

    scenario("check all fields are parsed correctly and sorted") {
      val reader = new PreMockFileReaderTest()
      Given("path to file")

      val path: String = ClassLoader.getSystemResource("configFiles/fileReader/configSample2.json").getPath

      Given("Result config for file configSample2")

      val resultFileContent =
        """{
          |  "namespace" : "",
          |  "name" : "checks",
          |  "doc" : "",
          |  "database" : "",
          |  "type" : "record",
          |  "fields" : [ {
          |    "name" : "field1PK",
          |    "type" : [ "string" ],
          |    "legacyName" : "field_1"
          |  }, {
          |    "name" : "field2PK",
          |    "type" : [ "string", "null" ],
          |    "legacyName" : "field_2"
          |  }, {
          |    "name" : "field3",
          |    "type" : [ "string" ],
          |    "legacyName" : "field_3"
          |  }, {
          |    "name" : "field4",
          |    "type" : [ "string", "null" ],
          |    "legacyName" : "field_4"
          |  } ]
          |}""".stripMargin


      When("Run config reader in path on config file")
      val fileContent = reader.getContentFromPath(s"file://$path")

      Then("Fields returned are correctly parsed")
      fileContent shouldBe resultFileContent

    }
  }
  feature("parse all lines in file from http schema") {
    scenario("check all lines in file are parsed") {
      Given("path to file")

      val reader = new PreMockFileReaderTest() {
        override def readFromHttp(path: String): String = CONTENT_OK
      }
      When("Run config reader in path with one config file")
      val configFile = reader.getContentFromPath("http://example.csv")

      Then("Return config for all fields")
      configFile.length shouldBe 21

    }
  }

  class PreMockFileReaderTest extends FileReader {
    val CONTENT_OK: String =
      """{
        |  "result": "OK"
        |}"""".stripMargin
    val EMPTY_FILE = ""
  }
}
