package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object InitNullsTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("initNullsWithAllNulls")
      .collect()
      .forall(
        _.getAs[String]("initNullsWithAllNulls") === "allNulls"
      )
    )

    assert(
      dfResult
        .select("initNullsWithAllNotNulls")
        .collect()
        .forall(
          _.getAs[String]("initNullsWithAllNotNulls") === "allNotNullsInit"
        )
    )

  }

}
