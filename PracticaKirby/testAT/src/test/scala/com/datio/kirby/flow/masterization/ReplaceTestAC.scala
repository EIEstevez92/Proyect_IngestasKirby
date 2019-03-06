package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object ReplaceTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("replaceValue")
      .collect()
      .forall(
        _.getAs[String]("replaceValue") === "replace"
      )
    )

  }


}
