package com.datio.kirby.api.util

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.util.RepartitionDsl._
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

case class Person(name: String, age: Int, city: String)

@RunWith(classOf[JUnitRunner])
class RepartitionDSLTest extends InitSparkSessionFeature with FileTestUtil with GivenWhenThen with Matchers {

  feature("Repartition Dataframe") {

    scenario("Repartition by num partitions") {

      val config = ConfigFactory.parseString(
        s"""
           |repartition {
           |  partitions = 10
           |}
        """.stripMargin
      )

      Given("A Dataframe with 3 partitions")
      val data = List(
        Person("Jesse", 23, "Ohio"),
        Person("John", 31, "Oklahoma"),
        Person("Sarah", 39, "Kansas")
      )

      val rdd = spark.sparkContext.parallelize(data, 3)
      val df = spark.createDataFrame(rdd)
      assert(df.rdd.getNumPartitions == 3)

      When("Repartition to 10 partitions")
      val dfResult = df.applyRepartitionIfNedeed()(config)

      Then("Result Dataframe has 10 partitions")
      assert(dfResult.rdd.getNumPartitions == 10)
    }

    scenario("Repartition by age using partition column") {

      implicit val config: Config = ConfigFactory.empty()
        .withValue("repartition.partitionBy", ConfigValueFactory.fromIterable(List("age").asJava))

      Given("A Dataframe with 3 partitions")
      val data = List(
        Person("Jesse", 23, "Ohio"),
        Person("John", 31, "Oklahoma"),
        Person("Sarah", 39, "Kansas"),
        Person("Kush", 31, "NY"),
        Person("Gwen", 43, "Denver")
      )

      val rdd = spark.sparkContext.parallelize(data, 3)
      val df = spark.createDataFrame(rdd)
      assert(df.rdd.getNumPartitions == 3)

      When("Repartition by a column (age)")
      val dfResult = df.applyRepartitionIfNedeed()

      Then("Result Dataframe has sql.shuffle.partitions partitions")
      val accum = spark.sparkContext.longAccumulator("PartitionFilled")
      dfResult.foreachPartition(it => {
        if (it.nonEmpty) accum.add(1L)
      })

      assert(spark.conf.get("spark.sql.shuffle.partitions").toInt == dfResult.rdd.getNumPartitions)
    }

    scenario("Repartition by age using partition column and number partitions") {


      implicit val config: Config = ConfigFactory.empty()
        .withValue("repartition.partitionBy", ConfigValueFactory.fromIterable(List("age").asJava))
        .withValue("repartition.partitions", ConfigValueFactory.fromAnyRef(4))

      Given("A Dataframe with 3 partitions")
      val data = List(
        Person("Jesse", 23, "Ohio"),
        Person("John", 31, "Oklahoma"),
        Person("Sarah", 39, "Kansas"),
        Person("Kush", 31, "NY"),
        Person("Gwen", 43, "Denver")
      )

      val rdd = spark.sparkContext.parallelize(data, 3)
      val df = spark.createDataFrame(rdd)
      assert(df.rdd.getNumPartitions == 3)

      When("Repartition to 4 partitions")
      val dfResult = df.applyRepartitionIfNedeed()

      Then("Result Dataframe has 4 partitions")
      assert(dfResult.rdd.getNumPartitions == 4)
    }

  }

  scenario("Coalesce: Decrease partitions") {

    implicit val config: Config = ConfigFactory.empty()
      .withValue("coalesce.partitions", ConfigValueFactory.fromAnyRef(2))

    Given("A Dataframe with 10 partitions")
    val data = List(
      Person("Jesse", 23, "Ohio"),
      Person("John", 31, "Oklahoma"),
      Person("Sarah", 39, "Kansas"),
      Person("Kush", 31, "NY"),
      Person("Gwen", 43, "Denver")
    )

    val rdd = spark.sparkContext.parallelize(data, 10)
    val df = spark.createDataFrame(rdd)
    assert(df.rdd.getNumPartitions == 10)

    When("Coalesce to 2 partitions")
    val dfResult = df.applyRepartitionIfNedeed()

    Then("Result Dataframe has 2 partitions")
    assert(dfResult.rdd.getNumPartitions == 2)
  }

  scenario("Coalesce: Should not increase number of partitions") {

    implicit val config: Config = ConfigFactory.empty()
      .withValue("coalesce.partitions", ConfigValueFactory.fromAnyRef(15))

    Given("A Dataframe with 10 partitions")
    val data = List(
      Person("Jesse", 23, "Ohio"),
      Person("John", 31, "Oklahoma"),
      Person("Sarah", 39, "Kansas"),
      Person("Kush", 31, "NY"),
      Person("Gwen", 43, "Denver")
    )

    val rdd = spark.sparkContext.parallelize(data, 10)
    val df = spark.createDataFrame(rdd)
    assert(df.rdd.getNumPartitions == 10)

    When("Coalesce to 15 partitions")
    val dfResult = df.applyRepartitionIfNedeed()

    Then("Result Dataframe has 10 partitions")
    assert(dfResult.rdd.getNumPartitions == 10)
  }

  scenario("Coalesce: Should return an error if has no partitions configuration") {

    val emptyMap = new java.util.HashMap[String, String]()
    implicit val config: Config = ConfigFactory.empty()
      .withValue("coalesce", ConfigValueFactory.fromMap(emptyMap))

    Given("A Dataframe with 10 partitions")
    val data = List(
      Person("Jesse", 23, "Ohio"),
      Person("John", 31, "Oklahoma"),
      Person("Sarah", 39, "Kansas"),
      Person("Kush", 31, "NY"),
      Person("Gwen", 43, "Denver")
    )

    val rdd = spark.sparkContext.parallelize(data, 10)
    val df = spark.createDataFrame(rdd)
    assert(df.rdd.getNumPartitions == 10)

    When("Coalesce to n partitions without partitions configuration")
    Then("Should return a KirbyException exception")
    intercept[KirbyException] {
      df.applyRepartitionIfNedeed()
    }
  }

}
