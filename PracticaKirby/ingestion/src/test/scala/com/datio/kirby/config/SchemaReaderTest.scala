package com.datio.kirby.config

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.fasterxml.jackson.core.JsonParseException
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class SchemaReaderTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with SchemaReader {

  val jsonSchemaOk: String =
    """{
      |  "namespace" : "",
      |  "name" : "checks",
      |  "doc" : "",
      |  "database" : "",
      |  "type" : "record",
      |  "fields" : [ {
      |    "name" : "field1PK",
      |    "type" : "string",
      |    "legacyName" : "field_1"
      |  }, {
      |    "name" : "field2PK",
      |    "type" : [ "string", "null" ],
      |    "legacyName" : "field_2"
      |  }, {
      |    "name" : "deleted1",
      |    "type" : "string",
      |    "default" : "",
      |    "legacyName" : "field_5",
      |    "deleted" : true
      |  }, {
      |    "name" : "field3",
      |    "type" : "float",
      |    "default" : 12.0,
      |    "legacyName" : "field_3"
      |  }, {
      |    "name" : "field4",
      |    "type" : "boolean",
      |    "default" : true,
      |    "legacyName" : "field_4",
      |    "deleted" : false
      |  }, {
      |    "name" : "deleted2",
      |    "type" : "boolean",
      |    "default" : true,
      |    "legacyName" : "field_6",
      |    "deleted" : true
      |  }, {
      |    "name" : "metadata",
      |    "type" : "String",
      |    "metadata" : true
      |  }
      | ]
      |}""".stripMargin

  val jsonSchemaOkWithBom = '\uFEFF' + jsonSchemaOk

  val schemaOk = StructType(Seq(
    StructField("field1PK", StringType, nullable = false),
    StructField("field2PK", StringType, nullable = true),
    StructField("field3", FloatType, nullable = false),
    StructField("field4", BooleanType, nullable = false)
  ))

  val schemaOkMetadata = StructType(Seq(
    StructField("field1PK", StringType, nullable = false),
    StructField("field2PK", StringType, nullable = true),
    StructField("field3", FloatType, nullable = false),
    StructField("field4", BooleanType, nullable = false),
    StructField("metadata", StringType, nullable = false)
  ))


  override def getContentFromPath(path: String): String = {
    path match {
      case "malformed1" => """{
                             |  "namespace" : "",
                             |  "name" : "checks",
                             |  "doc" : "",
                             |  "database" : "",
                             |  "type" : "record",
                             |  "fields" : [ {
                             |    "name" : "field1PK",
                             |    "type" : "string",
                             |    "default" : "",
                             |    "legacyName" : "field_1"
                             |  }, {
                             |    "name" : "field2PK",
                             |    "type" : [ "string", "null" ],
                             |  """.stripMargin
      case "malformed2" => """{
                             |  "namespace" : "",
                             |  "name" : "checks",
                             |  "doc" : "",
                             |  "database" : "",
                             |  "type" : "record",
                             |  "fields" : [ {
                             |    "name" : "field1PK",
                             |    "type" : "cadena",
                             |    "legacyName" : "field_1"
                             |  }
                             | ]
                             |}""".stripMargin
      case "empty" => "{}"
      case "jsonSchemaOkWithBom" => jsonSchemaOkWithBom
      case _ => jsonSchemaOk
    }
  }

  feature("Recover structType from String") {
    scenario("no validation no mandatory") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  validation = false
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"), mandatory = false)

      Then("Return empty structType")
      schema shouldBe StructType(Seq())
    }

    scenario("Mandatory without path and validation = false") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  validation = false
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      Then("Return empty structType")
      val e = intercept[KirbyException] {
        readSchema(inputConfig.getConfig("schema"))
      }
      e.getMessage should startWith(s"130 - Schema Validation Error: The schema.path is mandatory")
    }

    scenario("Mandatory without path and validation = false and env kirby_validation_enabled=false") {
      sys.props.put("kirby_validation_enabled", "false")
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  validation = false
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"), mandatory = false)

      Then("Return empty structType")
      schema shouldBe StructType(Seq())
      sys.props.remove("kirby_validation_enabled")
    }

    scenario("no mandatory with path") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "jsonSchemaOk"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"))

      Then("Return schema ok")
      schema shouldBe schemaOk
    }

    scenario("no mandatory with path and content with BOM") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "jsonSchemaOkWithBom"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"))

      Then("Return schema ok")
      schema shouldBe schemaOk
    }

    scenario("mandatory without path") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  validation = false
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      Then("Intercept correct exception")
      val e = intercept[KirbyException] {
        readSchema(inputConfig.getConfig("schema"))
      }
      e.getMessage should startWith(s"130 - Schema Validation Error: The schema.path is mandatory")
    }

    scenario("mandatory with json schema ok") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "jsonSchemaOk"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"))

      Then("Return schema ok")
      schema shouldBe schemaOk
    }

    scenario("mandatory with json schema ok and env kirby_validation_enabled=false") {
      sys.props.put("kirby_validation_enabled", "false")
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "jsonSchemaOk"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"))

      Then("Return schema ok")
      schema shouldBe schemaOk
      sys.props.remove("kirby_validation_enabled")
    }

    scenario("mandatory with json schema and metadata ok") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "jsonSchemaOk"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"),includeMetadata = true)

      Then("Return schema ok")
      schema shouldBe schemaOkMetadata
    }

    scenario("mandatory with empty json schema") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "empty"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      val schema = readSchema(inputConfig.getConfig("schema"))

      Then("Return empty structype")
      schema shouldBe StructType(Seq())
    }

    scenario("mandatory with malformed json") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "malformed1"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      Then("Intercept JsonParseException")
      intercept[JsonParseException] {
        readSchema(inputConfig.getConfig("schema"))
      }

    }

    scenario("mandatory with not allowed type") {
      val inputConfig = ConfigFactory.parseString(
        s"""
           |schema {
           |  path = "malformed2"
           |}
        """.stripMargin
      )

      When("Generate Structype from content")
      Then("Intercept correct exception")
      val e = intercept[KirbyException] {
        readSchema(inputConfig.getConfig("schema"))
      }
      e.getMessage shouldBe "113 - Conversion Util Error: Format cadena has not been implemented"
    }

  }
}

