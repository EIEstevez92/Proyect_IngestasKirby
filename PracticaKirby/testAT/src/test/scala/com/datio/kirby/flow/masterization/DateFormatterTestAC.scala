package com.datio.kirby.flow.masterization

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object DateFormatterTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    val correctDate = new java.sql.Date(new SimpleDateFormat("dd/MM/yyy").parse("01/01/2017").getTime)

    dfResult.count() shouldBe 7

    assert(
      dfResult
        .select("correctDate")
        .collect()
        .forall(
          _.getAs[Date]("correctDate") === correctDate
        )
    )

    assert(
      dfResult
        .select("inCorrectDate")
        .collect()
        .forall(
          _.getAs[String]("inCorrectDate") === null
        )
    )

  }

}
