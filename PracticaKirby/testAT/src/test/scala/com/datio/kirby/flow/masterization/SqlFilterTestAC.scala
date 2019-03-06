package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object SqlFilterTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    assert(dfResult.count == 2L)

  }

}
