package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object UpdateTimeTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    assert(dfResult.columns.contains("currentDate"))

    assert(dfResult.groupBy("currentDate").count().collect().length == 1)

  }

}
