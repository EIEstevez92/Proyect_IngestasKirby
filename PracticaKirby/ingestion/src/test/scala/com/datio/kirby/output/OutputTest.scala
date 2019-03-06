package com.datio.kirby.output

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.errors.{OUTPUT_REPROCESS_ABORTED, OUTPUT_REPROCESS_PARTITION_BLOCKED, OUTPUT_OVERWRITE_BLOCKED, REPROCESS_EMPTY_LIST, REPROCESS_EMPTY_PARTITION_LIST, REPROCESS_MISSED_PARTITION}
import com.datio.kirby.config.{InputFactory, OutputFactory}
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature, TestEntityForPartition}
import com.datio.spark.metric.utils.ProcessInfo
import com.typesafe.config.ConfigFactory
import org.apache.spark.metric.DefaultListener
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen, Matchers}

trait OutputTest extends InitSparkSessionFeature with FileTestUtil with GivenWhenThen with Matchers

  with OutputFactory with BeforeAndAfterEach with InputFactory {

  val outputType: String

  def readData(path: String): DataFrame

  val exampleList = List(
    TestEntityForPartition("hello", "1", "123"),
    TestEntityForPartition("hello", "2"),
    TestEntityForPartition("hello", "3"),
    TestEntityForPartition("world", "1", "345"),
    TestEntityForPartition("world", "2"),
    TestEntityForPartition("world", "3"),
    TestEntityForPartition("!!", "1", "7567"),
    TestEntityForPartition("!!", "2", "564"),
    TestEntityForPartition("!!", "3", "gdf")
  )
  val schema = com.datio.kirby.testUtils.schemaTestEntityForPartition

  private val field1 = "text"
  private val field2 = "text2"
  private val field3 = "text3"
  private val field4 = "text4"

  private def path = s"src/test/resources/output/example_mae.$outputType"

  override def afterEach(): Unit = {
    delete(path)
  }

  feature("Try to Save a file in a temporal path") {

    scenario("without partition") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |  }
        """.stripMargin)
      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true

      result = true
    }

    scenario("with partition") {
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    partition = [
           |       $field1
           |    ]
           |  }
        """.stripMargin)
      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true
    }

    scenario("with two partition") {
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    partition = [
           |       $field1,
           |       $field2
           |    ]
           |  }
        """.stripMargin)
      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true
    }
  }

  feature("Try to overwrite a file in a temporal path") {

    scenario("with force without partition") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    mode = "overwrite"
           |    force = true
           |  }
        """.stripMargin)

      createFile(path)


      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true

      result = true
    }

    scenario("with force with partition") {
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    mode = "overwrite"
           |    force = true
           |    partition = [
           |       $field1
           |    ]
           |  }
        """.stripMargin)

      createFile(path)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true
    }

    scenario("with force with two partition") {
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    mode = "overwrite"
           |    force = true
           |    partition = [
           |       $field1,
           |       $field2
           |    ]
           |  }
        """.stripMargin)

      createFile(path)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true
    }

    scenario("with overwrite to false") {
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    partition = [
           |       $field1,
           |       $field2
           |    ]
           |  }
        """.stripMargin)

      createFile(path)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))

      val exception = intercept[AnalysisException] {
        writer.write(df)
      }

      Then("Exception is thrown")
      exception.message should fullyMatch regex """path .* already exists."""
    }

    scenario("without force") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = $path
           |    mode = "overwrite"
           |  }
        """.stripMargin)

      createFile(path)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      Then("thrown KirbyApiException")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      caught.getMessage shouldBe OUTPUT_OVERWRITE_BLOCKED.toFormattedString("overwrite",s"src/test/resources/output/example_mae.$outputType")
    }

    scenario("without force in a partition") {
      testCase = "CD-11"
      Given("Get the data")
      val partitionPath = path + "/part=0034-23"
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$partitionPath"
           |    mode = "overwrite"
           |  }
        """.stripMargin)

      createDir(path)
      createFile(partitionPath)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      Then("thrown KirbyApiException")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      caught.getMessage shouldBe OUTPUT_REPROCESS_PARTITION_BLOCKED.toFormattedString(partitionPath)
    }

    scenario("with force in a partition") {
      testCase = "CD-11"
      Given("Get the data")
      val partitionPath = path + "/part=0034-23"
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$partitionPath"
           |    mode = "overwrite"
           |    force = true
           |  }
        """.stripMargin)

      createDir(path)
      createFile(partitionPath)

      import spark.implicits._
      val df = exampleList.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(partitionPath) shouldBe true

      result = true
    }

  }

  feature("Reprocess a partition") {

    scenario("with one reprocess and one partition") {
      testCase = "CD-11"
      Given("A prepared dataframe partitioned")
      val prepareDataConfig = ConfigFactory.parseString(
        s"""
           |    type = "$outputType"
           |    path = $path
           |    force = true
           |    partition = [ $field1 ]
        """.stripMargin)

      import spark.implicits._
      val prepareDatawriter = readOutput(prepareDataConfig)
      prepareDatawriter.write(exampleList.toDF)
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello" ]
           |    partition = [ $field1 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123")).filter(_.text == "hello")
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true

      Then("data must be correct")
      val storedData = readData(path).collect().map(row => {
        TestEntityForPartition(row.getAs[String](field1), row.getAs[String](field2), row.getAs[String](field3), row.getAs[String](field4))
      })
      val expectedData = exampleList.filter(_.text != "hello") ++ newData
      storedData.toSet shouldBe expectedData.toSet

      result = true
    }

    scenario("with two reprocess and tree partitions") {
      testCase = "CD-11"
      Given("A prepared dataframe partitioned")
      val prepareDataConfig = ConfigFactory.parseString(
        s"""
           |    type = "$outputType"
           |    path = $path
           |    force = true
           |    partition = [ $field1, $field2, $field3 ]
        """.stripMargin)

      import spark.implicits._
      val prepareDatawriter = readOutput(prepareDataConfig)
      prepareDatawriter.write(exampleList.toDF)
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello/$field2=1", "$field1=hello/$field2=not-exists" ]
           |    partition = [ $field1, $field2, $field3 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123"), TestEntityForPartition("hello", "not-exists", "3"))
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true

      Then("data must be correct")
      val storedData = readData(path).collect().map(row => {
        TestEntityForPartition(row.getAs[String](field1), row.getAs[String](field2), row.getAs[String](field3), row.getAs[String](field4))
      })
      val expectedData = exampleList.filter(e => !(e.text == "hello" && e.text2 == "1") && !(e.text == "hello" && e.text2 == "not-exists")) ++ newData
      storedData.toSet shouldBe expectedData.toSet

      result = true
    }

    scenario("with tree reprocess and tree partitions") {
      testCase = "CD-11"
      Given("A prepared dataframe partitioned")
      val prepareDataConfig = ConfigFactory.parseString(
        s"""
           |    type = "$outputType"
           |    path = $path
           |    force = true
           |    partition = [ $field1, $field2, $field3 ]
        """.stripMargin)

      import spark.implicits._
      val prepareDatawriter = readOutput(prepareDataConfig)
      prepareDatawriter.write(exampleList.toDF)
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello/$field2=1", "$field1=hello/$field2=2", "$field1=world" ]
           |    partition = [ $field1, $field2, $field3 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123"), TestEntityForPartition("world", "n", "1"), TestEntityForPartition("hello", "2", "1"))
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      writer.write(df)

      Then("File should exits")
      exists(path) shouldBe true

      Then("data must be correct")
      val storedData = readData(path).collect().map(row => {
        TestEntityForPartition(row.getAs[String](field1), row.getAs[String](field2), row.getAs[String](field3), row.getAs[String](field4))
      })
      val expectedData = exampleList.filter(e => !(e.text == "hello" && e.text2 == "1") && !(e.text == "hello" && e.text2 == "2") && e.text != "world") ++ newData
      storedData.toSet shouldBe expectedData.toSet

      result = true
    }

    scenario("with missed reprocess element") {
      testCase = "CD-11"

      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    partition = [ $field1 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)
      import spark.implicits._
      val newData = List(TestEntityForPartition("hello", "newData", "123")).filter(_.text == "hello")
      val df = newData.toDF

      When("Run the writer in path")
      Then("thrown KirbyApiException")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      caught.getMessage shouldBe REPROCESS_EMPTY_LIST.toFormattedString()
      result = true
    }

    scenario("with partitions empty") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello" ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123"), TestEntityForPartition("world", "newData", "345"),
        TestEntityForPartition("!!", "newData", "7567")).filter(_.text == "hello")
      import spark.implicits._
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      Then("thrown correct exception")
      caught.getMessage shouldBe REPROCESS_EMPTY_PARTITION_LIST.toFormattedString()
      result = true
    }

    scenario("with bad order in reprocess") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello", "$field2=hello/$field1=other" ]
           |    partition = [ $field1, $field2 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123"), TestEntityForPartition("world", "newData", "345"),
        TestEntityForPartition("!!", "newData", "7567")).filter(_.text == "hello")
      import spark.implicits._
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      Then("thrown correct exception")
      caught.getMessage shouldBe REPROCESS_MISSED_PARTITION.toFormattedString("text, text2", "text2=hello/text=other")
      result = true
    }

    scenario("with invented reprocess partition") {
      testCase = "CD-11"
      Given("Get the data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | output {
           |    type = "$outputType"
           |    path = "$path"
           |    reprocess = [ "$field1=hello/another=other" ]
           |    partition = [ $field1 ]
           |    mode = "reprocess"
           |  }
        """.stripMargin)

      val newData = List(TestEntityForPartition("hello", "newData", "123"), TestEntityForPartition("world", "newData", "345"),
        TestEntityForPartition("!!", "newData", "7567")).filter(_.text == "hello")
      import spark.implicits._
      val df = newData.toDF

      When("Run the writer in path")
      val writer = readOutput(myConfig.getConfig("output"))
      val caught =
        intercept[KirbyException] {
          writer.write(df)
        }
      Then("thrown correct exception")
      caught.getMessage shouldBe REPROCESS_MISSED_PARTITION.toFormattedString("text", "text=hello/another=other")
      result = true
    }

  }
  feature("storing safely an output previously reprocessed ") {

    scenario("comparing an initial and reprocessed test files ") {

      testCase = "CD-11"

      val listener = new DefaultListener()

      listener.result.runId = "test"
      listener.result.time_init = 2
      listener.result.time_end = 3
      listener.result.recordsRead = 6
      listener.result.recordsWritten = 4
      listener.result.bytesRead = 6
      listener.result.bytesWritten = 7


      Given("storing an initial test dataframe")
      val prepareDataConfig = ConfigFactory.parseString(
        s"""
           |    type = "$outputType"
           |    path = $path
           |    force = true
           |    mode = overwrite
           |    delimiter = ";"
        """.stripMargin)

      import spark.implicits._
      val prepareDatawriter = readOutput(prepareDataConfig)
      prepareDatawriter.write(exampleList.toDF)

      Given("Storing safely the DF with a new output configuration")
      val myConfig2Write = ConfigFactory.parseString(
        s"""
           | sparkMetrics {
           |    listeners = ["default"]
           |    output {
           |       type = "console"
           |    }
           | }
           | kirby {
           |    output {
           |       type = "$outputType"
           |       path = "$path"
           |       force = true
           |       mode = overwrite
           |       externalExtraLines = 2
           |       safeReprocess = true
           |    }
           | }
        """.stripMargin)

      When("Run the writer in path")
      val writer = readOutput(myConfig2Write.getConfig("kirby.output"))
      writer.write(exampleList.toDF, new ProcessInfo(listener))

      Then("Final file should exits")
      exists(path) shouldBe true

      Then("data must be correct")
      val storedData = readData(path).toDF().orderBy(field3, field1, field4, field2).collect()
      val orderExampleList = exampleList.toDF("col1", "col2", "col3", "col4").orderBy("col3", "col1", "col4", "col2").collect()
      storedData shouldBe orderExampleList

      result = true
    }

    scenario("Exception due to not safe storing") {

      testCase = "CD-11"

      val listener = new DefaultListener()

      listener.result.runId = "test"
      listener.result.time_init = 2
      listener.result.time_end = 3
      listener.result.recordsRead = 6
      listener.result.recordsWritten = 10
      listener.result.bytesRead = 6
      listener.result.bytesWritten = 7


      Given("Reading the previos DF")

      import spark.implicits._
      val fields: DataFrame = exampleList.toDF

      Given("Storing safely the DF with a new output configuration")
      val myConfig2Write = ConfigFactory.parseString(
        s"""
           | sparkMetrics {
           |    listeners = ["default"]
           |    output {
           |       type = "console"
           |    }
           | }
           | kirby {
           |    output {
           |       type = "$outputType"
           |       path = "$path"
           |       force = true
           |       mode = overwrite
           |       safeReprocess = true
           |    }
           | }
        """.stripMargin)

      When("Run the writer in path")
      val writer = readOutput(myConfig2Write.getConfig("kirby.output"))
      val caught =
        intercept[KirbyException] {
          writer.write(fields, new ProcessInfo(listener))
        }

      Then("thrown correct exception")
      caught.getMessage shouldBe OUTPUT_REPROCESS_ABORTED.toFormattedString()
      result = true
    }
  }
}