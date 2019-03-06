package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object ExtractInfoFromDateTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    val a = dfResult.collect()

    dfResult.count() shouldBe 7

    assert(
      dfResult
        .select("day")
        .collect()
        .forall(
          _.getAs[String]("day") === "1"
        )
    )

    assert(
      dfResult
        .select("month")
        .collect()
        .forall(
          _.getAs[String]("month") === "2"
        )
    )

    assert(
      dfResult
        .select("year")
        .collect()
        .forall(
          _.getAs[String]("year") === "2017"
        )
    )

    assert(
      dfResult
        .select("badday")
        .collect()
        .forall(
          _.getAs[String]("badday") === null
        )
    )

    assert(
      dfResult
        .select("badmonth")
        .collect()
        .forall(
          _.getAs[String]("badmonth") === null
        )
    )

    assert(
      dfResult
        .select("badyear")
        .collect()
        .forall(
          _.getAs[String]("badyear") === null
        )
    )

  }


}
