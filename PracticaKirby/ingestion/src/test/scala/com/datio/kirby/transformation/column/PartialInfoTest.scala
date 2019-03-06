package com.datio.kirby.transformation.column

import java.sql.Date
import java.util.Calendar

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class PartialInfoTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("literal column") {

    scenario("extract relevance info from string") {
      Given("config for partialinfo transformation")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "partialInfoField"
          |        type = "partialInfo"
          |        fieldInfo = "text"
          |        start = "5"
          |        length = "13"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse: List[Row] = Row("aaaaRelevanceInfoaaaa") :: Row("BBBBRelevanceInfoBBBB") :: Row("CC22RelevanceInfoCC11") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("add partialInfoField with relevance info")
      assert(dfResult
        .select("partialInfoField")
        .collect()
        .forall(
          _.getAs[String]("partialInfoField") === "RelevanceInfo"
        )
      )

      And("Other field should exist")
      assert(dfResult
        .select("text")
        .collect()
        .map(_.getAs[String]("text"))
        .sameElements(df.collect().map(_.getAs[String]("text")))
      )
    }

    scenario("extract relevance info from date") {
      Given("config for partialinfo transformation")

      val config = ConfigFactory.parseString(
        """
          |      {
          |         field = "partialInfoField"
          |        type = "partialInfo"
          |        fieldInfo = "date"
          |        start = "6"
          |        length = "2"
          |      }
        """.stripMargin
      )


      val calendar: Calendar = Calendar.getInstance()
      calendar.set(Calendar.MONTH, Calendar.JULY)
      val dateToTest: Date = new Date(calendar.getTime.getTime)


      Given("column to parse")

      val columnToParse: List[Row] = Row(dateToTest) :: Row(dateToTest) :: Row(dateToTest) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("date", DateType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("add partialInfoField with month")
      assert(dfResult
        .select("partialInfoField")
        .collect()
        .forall(
          _.getAs[String]("partialInfoField") === "07"
        )
      )

      And("Other field should exist")
      assert(dfResult
        .select("date")
        .collect()
        .map(_.getAs[Date]("date"))
        .sameElements(df.collect().map(_.getAs[Date]("date")))
      )

    }

  }
}
