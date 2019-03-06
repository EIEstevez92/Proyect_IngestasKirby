package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object CopyColumnTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("copyofothercolumn")
      .collect()
      .forall(
        _.getAs[String]("copyofothercolumn") === "1"
      )
    )

    assert(
      dfResult
        .select("copyofothercolumnwithcast")
        .collect()
        .forall(
          _.getAs[Int]("copyofothercolumnwithcast") === 1
        )
    )

  }

}
