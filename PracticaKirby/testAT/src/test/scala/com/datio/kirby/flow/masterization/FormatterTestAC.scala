package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, DecimalType, LongType, StringType}
import org.scalatest.{FunSuite, Matchers}


object FormatterTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    dfResult.schema.find(_.name == "stringToString").get.dataType shouldBe StringType
    dfResult.schema.find(_.name == "stringToLong").get.dataType shouldBe LongType
    dfResult.schema.find(_.name == "stringToDate").get.dataType shouldBe DateType
    dfResult.schema.find(_.name == "stringToDecimal").get.dataType shouldBe DecimalType(2,0)

  }

}
