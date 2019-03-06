package com.datio.kirby.input

import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class XmlInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  feature("check if the files can be readed") {
    scenario("Try to read a file with a not valid path") {
      testCase = "CD-3"
      Given("path to file")

      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/badExampleFile.xml"]
           |    delimiter = ";"
           |  }
        """.stripMargin)

      When("Try to read the file")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val thrown: Exception = intercept[Exception] {
        reader.read(spark).count
      }

      Then("there is a null exception")
      assert(thrown.getMessage.nonEmpty)

      result = true
    }

    scenario("read the file applying a valid schema") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/example_mae.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = row
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 14

      result = true
    }

    scenario("read the file applying a valid schema for books") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/books.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = book
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaBooksGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 12

      result = true
    }

    scenario("read the file applying a valid schema for acceptance input") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/acceptance_input.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = Organization_Root
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaXmlAcceptanceInputGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 11


      result = true
    }


    scenario("read two files applying a valid schema") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/example_mae.xml","src/test/resources/configFiles/xml/example_mae.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = row
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 28

      result = true
    }

    scenario("read the file applying a not valid schema") {
      testCase = "CD-45"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/example_mae.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = row2
           |    }
           |  }
        """.stripMargin)
      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaBad
      }
      val fields = reader.read(spark)

      Then("Not Return any valid row")
      fields.count shouldBe 0

      result = true
    }

    scenario("read the file applying a valid schema for books with upper case") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/books2.xml"]
           |    columnMapping.path = "src/test/resources/configFiles/xml/columnMappingsBooks2.csv"
           |    delimiter = ";"
           |    options {
           |        rowTag = book
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaBooksGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 12
      for {
        row <- fields.collect()
        structField <- Schemas.schemaBooksGood
      } yield row.getAs[AnyRef](structField.name) should not be null

      result = true
    }

    scenario("read the file applying a valid schema for instruments with upper case") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "xml"
           |    paths = ["src/test/resources/configFiles/xml/instrumentTest.xml"]
           |    delimiter = ";"
           |    options {
           |        rowTag = Instrument_Rating_Attribute
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new XmlInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaInstrumentsGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 1
      for {
        row <- fields.collect()
      } yield row.size shouldBe Schemas.schemaInstrumentsGood.size

      result = true
    }
  }
}
