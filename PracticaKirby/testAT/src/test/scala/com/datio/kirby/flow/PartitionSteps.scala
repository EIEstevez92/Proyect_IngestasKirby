package com.datio.kirby.flow

import com.datio.kirby.read.{CheckPartitionsReader, CheckPathReader, CreatePath}
import cucumber.api.scala.{EN, ScalaDsl}
import org.scalatest.Matchers

class PartitionSteps extends Matchers with ScalaDsl with EN {

  var storedPath: String = ""

  Then("""^should be stored in (.*)$""") {
    (path: String) =>
      storedPath = path
      CheckPathReader.check(path) shouldBe true
  }

  And("""^should exists files partitioned by (.*)$""") {
    (partionedBy: String) =>
      CheckPartitionsReader.check(storedPath, partionedBy) shouldBe true
  }

  And("""^should not exist this (.*)$""") {
    (partionedBy: String) =>
      CheckPartitionsReader.check(storedPath, partionedBy) shouldBe false
  }

  And("""^existing a file in (.*)$""") {
    (path: String) =>
      CreatePath.create(path) shouldBe true
  }

}