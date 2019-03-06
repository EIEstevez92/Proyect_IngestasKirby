package com.datio.kirby.custom

import com.datio.kirby.read.JSONReader
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.Row
import org.scalatest.Matchers


class CustomVerify extends Matchers with ScalaDsl with EN {

  Then("""should produce custom result in (.*)""") {
    (pathDest: String) => {

      val dfResult = JSONReader.read(pathDest)

      dfResult.count() shouldBe 4

      val keys = Seq("KEY1", "KEY2", "KEY3", "KEY4")
      val values = Seq("value1", "value2", "value3", "value4")

      val indexedResult = dfResult
        .collect()
        .zipWithIndex

      assert(
        indexedResult.forall(
          _ match {
            case (row: Row, index: Int) => row.getAs[String]("key") === keys(index) &&
              row.getAs[String]("value") === values(index)
            case _ => false
          }
        )
      )
    }
  }

}
