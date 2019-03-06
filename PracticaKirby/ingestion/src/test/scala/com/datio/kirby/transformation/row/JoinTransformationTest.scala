package com.datio.kirby.transformation.row

import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class JoinTransformationTest extends InitSparkSessionFeature with GivenWhenThen with Matchers {

  val schema = StructType(
    StructField("text", StringType) ::
      StructField("dropColumn", StringType) :: Nil
  )

  feature("Join Transformation") {
    scenario("join columns correctly with one table") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="x"
          |          }
          |        ]
          |      }]
          |      resolveConflictsAuto = true
          |      select = ["self.x","self.y", "self.z","t1.z","t1.`j(strange)j` as j"]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j(strange)j", "z")
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "z")

      Then("result dataframe must be the expected")
      val expectedResult = Set(Row(1, 2, 3, 2, 3), Row(1, 1, 3, 2, 3), Row(2, 2, 3, 2, 3), Row(1, 1, 3, 1, 3), Row(1, 2, 3, 1, 3))
      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with one table applying transformations") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="x"
          |          }
          |        ]
          |        transformations = [
          |          {
          |            type = "filter"
          |            filters: [{
          |              field = "x"
          |              value = 2
          |              op = "eq"
          |            }]
          |          }
          |        ]
          |      }]
          |      resolveConflictsAuto = true
          |      select = ["self.x","self.y", "self.z","t1.z","t1.`j(strange)j` as j"]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j(strange)j", "z")
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "z")

      Then("result dataframe must be the expected")
      val expectedResult = Set(Row(1, 1, 3, null, null), Row(1, 2, 3, null, null), Row(2, 2, 3, 2, 3))
      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with two tables") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |  {
          |    joins =
          |      [
          |        {
          |          input = {
          |            key=1
          |          }
          |          alias = t1
          |          joinType = "left"
          |          joinColumns = [
          |            {
          |              self= "x"
          |              other="x"
          |            }
          |          ]
          |        },
          |        {
          |          input = {
          |            key=2
          |          }
          |          alias = t2
          |          joinType = "inner"
          |          joinColumns = [
          |            {
          |              self= "t1.y"
          |              other="y"
          |            }
          |          ]
          |        }
          |      ]
          |    resolveConflictsAuto = true
          |    select = ["self.x","self.y", "self.z","t1.x as x1","t1.y as y1","t1.z as z1","t2.x as x2","t2.y as y2","t2.z as z2"]
          |    type = "join"
          |  }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("Two join dataframes")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          if (input.getInt("key") == 1) {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")
          } else {
            List((5, 6, 7), (7, 3, 3), (2, 2, 13)).toDF("x", "y", "z")
          }
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "y", "z1", "x2", "z2", "y1", "x1", "z", "y2")

      Then("result dataframe must be the expected")
      val expectedResult = Set(Row(1, 1, 3, 1, 2, 3, 2, 2, 13), Row(1, 2, 3, 1, 2, 3, 2, 2, 13), Row(2, 2, 3, 2, 2, 3, 2, 2, 13))
      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with two tables applying transformations") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |  {
          |    joins =
          |      [
          |        {
          |          input = {
          |            key=1
          |          }
          |          alias = t1
          |          joinType = "left"
          |          joinColumns = [
          |            {
          |              self= "x"
          |              other="x"
          |            }
          |          ]
          |          transformations = [
          |            {
          |              type = "formatter"
          |              field = "z"
          |              typeToCast = "string"
          |              replacements: [{
          |                pattern = "[0-9]"
          |                replacement = "test"
          |              }]
          |            }
          |          ]
          |        },
          |        {
          |          input = {
          |            key=2
          |          }
          |          alias = t2
          |          joinType = "inner"
          |          joinColumns = [
          |            {
          |              self= "t1.y"
          |              other="y"
          |            }
          |          ]
          |          transformations = [
          |            {
          |              type = "filter"
          |              filters: [{
          |                field = "x"
          |                value = 2
          |                op = "gt"
          |              }]
          |            },
          |            {
          |              type = "formatter"
          |              field = "y"
          |              typeToCast = "integer"
          |              replacements: [{
          |                pattern = "(\\d{4})-(\\d{2})-(\\d{2})"
          |                replacement = "$1"
          |              }]
          |            }
          |          ]
          |        }
          |      ]
          |    resolveConflictsAuto = true
          |    select = ["self.x","self.y", "self.z","t1.x as x1","t1.y as y1","t1.z as z1","t2.x as x2","t2.y as y2","t2.z as z2"]
          |    type = "join"
          |  }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("Two join dataframes")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          if (input.getInt("key") == 1) {
            List((1, 2020, 3), (1, 2019, 3), (1, 2019, 3), (2, 2018, 3)).toDF("x", "y", "z")
          } else {
            List((9, "2020-07-16", 7), (1, "2019-07-16", 7), (3, "2018-07-16", 3), (2, "2017-07-16", 13)).toDF("x", "y", "z")
          }
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "y", "z1", "x2", "z2", "y1", "x1", "z", "y2")

      Then("result dataframe must be the expected")
      val expectedResult = Set(Row(1, 1, 3, 1, 2020, "test", 9, 2020, 7),
        Row(1, 2, 3, 1, 2020, "test", 9, 2020, 7), Row(2, 2, 3, 2, 2018, "test", 3, 2018, 3))
      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with one table and without 'select'") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="x"
          |          }
          |        ]
          |      }]
          |      resolveConflictsAuto = true
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "t1_x", "z")

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with self") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = "self"
          |        alias = me
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="x"
          |          }
          |        ]
          |      }]
          |      resolveConflictsAuto = true
          |      select = ["self.x", "self.y", "self.z", "me.x", "me.y", "me.z"]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config).transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "me_x", "y", "me_z", "me_y", "z")

      Then("result dataframe must be the expected")
      val expectedResult = Set(Row(2, 2, 3, 2, 2, 3), Row(1, 2, 3, 1, 1, 3), Row(1, 1, 3, 1, 1, 3), Row(1, 1, 3, 1, 2, 3), Row(1, 2, 3, 1, 2, 3))
      dfCleaned.collect.map(row => row).toSet shouldBe expectedResult

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("join columns correctly with one table and using * on select") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="x"
          |          }
          |        ]
          |      }]
          |      resolveConflictsAuto = true
          |      select = ["t1.*", "self.*"]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      val dfCleaned = new JoinTransformation(config) {
        override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
          List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
        }
      }.transform(dfOrigin)

      Then("result column names must be the selected columns")
      dfCleaned.columns.toSet shouldBe Set("x", "t1_z", "j", "y", "t1_x", "z")

      Then("There aren't column names repeated")
      dfCleaned.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet shouldBe Set()
    }

    scenario("pass invalid configuration with 'joins' empty") {
      Given("A configuration")
      val configJoinEmpty = ConfigFactory.parseString(
        """
          |{
          |  joins = []
          |  type = "join"
          |}
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(configJoinEmpty) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: 'joins' cannot be empty"
    }

    scenario("pass invalid configuration with 'joins' not defined") {
      Given("A configuration")
      val configJoinEmpty = ConfigFactory.parseString(
        """
          |{
          |  type = "join"
          |}
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(configJoinEmpty) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: 'joins' is mandatory"
    }

    scenario("pass invalid configuration with 'joinColumns' empty") {
      Given("A configuration")
      val configJoinColumnsEmpty = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = []
          |      }]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(configJoinColumnsEmpty) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: 'joinColumns' cannot be empty"
    }


    scenario("pass invalid configuration with 'joinColumns' with invalid columns") {
      Given("A configuration")
      val configJoinColumnsInvalid = ConfigFactory.parseString(
        """
          |{
          |      joins = [ {
          |        input = {}
          |        alias = t1
          |        joinType = "left"
          |        joinColumns = [
          |          {
          |            self= "x"
          |            other="h"
          |          }
          |        ]
          |      }]
          |      type = "join"
          |    }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(configJoinColumnsInvalid) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: 'joinColumns' contains unknown columns"
    }

    scenario("pass invalid configuration with 'joinColumns' with duplicated aliases") {
      Given("A configuration")
      val configAliasesDuplicated = ConfigFactory.parseString(
        """
          |  {
          |    joins =
          |      [
          |        {
          |          input = {
          |            key=1
          |          }
          |          alias = t1
          |          joinType = "left"
          |          joinColumns = [
          |            {
          |              self= "x"
          |              other="x"
          |            }
          |          ]
          |        },
          |        {
          |          input = {
          |            key=2
          |          }
          |          alias = t1
          |          joinType = "inner"
          |          joinColumns = [
          |            {
          |              self= "t1.y"
          |              other="y"
          |            }
          |          ]
          |        }
          |      ]
          |    select = ["self.x","self.y", "self.z","t1.x","t1.y","t1.z","t2.x","t2.x","t2.z"]
          |    type = "join"
          |  }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("A join dataframe")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(configAliasesDuplicated) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: aliases must be unique"
    }

    scenario("pass invalid select with duplicated aliases") {
      Given("A configuration")
      val config = ConfigFactory.parseString(
        """
          |  {
          |    joins =
          |      [
          |        {
          |          input = {
          |            key=1
          |          }
          |          alias = t1
          |          joinType = "left"
          |          joinColumns = [
          |            {
          |              self= "x"
          |              other="x"
          |            }
          |          ]
          |        }
          |      ]
          |    resolveConflictsAuto = true
          |    select = ["self.x as x1", "t1.x as x1"]
          |    type = "join"
          |  }
        """.stripMargin)

      Given("A Input Dataframe")
      import spark.implicits._
      val dfOrigin = List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "y", "z")

      Given("Two join dataframes")
      When("Perform the join")
      Then("throws correct exception")
      val caught = intercept[JoinTransformationException] {
        new JoinTransformation(config) {
          override def getInput(sparkSession: SparkSession, alias: String, input: Config): DataFrame = {
            List((1, 1, 3), (1, 2, 3), (2, 2, 3)).toDF("x", "j", "z")
          }
        }.transform(dfOrigin)
      }

      caught.message shouldBe "JoinTransformation: There are duplicated columns in output that need to be resolved manually: x1"
    }
  }

}