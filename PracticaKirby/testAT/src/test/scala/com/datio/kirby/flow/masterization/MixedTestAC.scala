package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object MixedTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    dfResult.count() shouldBe 5

    assert(dfResult.columns.length == 6)

    assert(dfResult.
      columns.
      forall(
        Array("field_1", "field_2", "field4Renamed", "date", "baddate", "field1PK").contains(_)
      ))

    assert(!dfResult.columns.contains("field Renamed"))

    assert(
      dfResult
        .select("field1PK")
        .collect()
        .forall(
          _.getAs[String]("field1PK") === "test of trim"
        )
    )

    assert(
      dfResult
        .groupBy("field4Renamed")
        .count()
        .collect()
        .forall(v => v.get(0) match {
          case "allColumnsDuplicate" => v.get(1) === 1
          case "field4Renamed" => v.get(1) === 3
          case null => v.get(1) === 1
          case _ => false
        }
        )
    )
  }

}
