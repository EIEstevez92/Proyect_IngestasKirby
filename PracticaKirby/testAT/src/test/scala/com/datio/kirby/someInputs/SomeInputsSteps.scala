package com.datio.kirby.someInputs

import com.datio.kirby.read.AvroReader
import cucumber.api.scala.{EN, ScalaDsl}
import com.datio.kirby.read.ParquetReader
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

class SomeInputsSteps extends Matchers with ScalaDsl with EN {

  var df : DataFrame = _
  var linesCompleted : Long = 0L

  Then("""^should produce avro output with double lines in destPath (.*)$""") {
    (pathOutput : String) =>

      df = AvroReader.read(pathOutput)

  }

  Then("""^should produce parquet output with double lines in destPath (.*)$""") {
    (pathOutput : String) =>

      df = ParquetReader.read(pathOutput)

  }

  And("""^should contain double of (\d+) lines$""") {
    (lines : Int) =>

      linesCompleted = df.count()

      linesCompleted shouldBe ( lines * 2L )
  }

}
