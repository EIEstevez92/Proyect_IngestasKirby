package com.datio.kirby.util

import com.datio.kirby.api.util.FileUtils
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class FileUtilsTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with FileTestUtil with FileUtils {

  private def path = s"src/test/resources/fileUtilsTest"

  def createPartitionTree(): Unit = {
    createDir(path)
    createDir(s"$path/_SUCCESS")
    createDir(s"$path/._SUCCESS.crc")
    createDir(s"$path/closing_date=12-10-01")
    createDir(s"$path/closing_date=12-10-01/hour=10")
    createFile(s"$path/closing_date=12-10-01/hour=10/test.csv")
    createDir(s"$path/closing_date=12-10-01/hour=11")
    createFile(s"$path/closing_date=12-10-01/hour=11/test.csv")
    createDir(s"$path/closing_date=12-10-02")
    createDir(s"$path/closing_date=12-10-02/hour=10")
    createFile(s"$path/closing_date=12-10-02/hour=10/test.csv")
    createDir(s"$path/closing_date=12-10-03")
    createDir(s"$path/closing_date=12-10-03/hour=11")
    createFile(s"$path/closing_date=12-10-03/hour=11/test.csv")
  }

  feature("get partitions correctly") {
    scenario("directory with partition") {
      Given("table with partition")
      createPartitionTree()
      When("get partitions")
      val partitions = getPartitions(path)
      Then("get the partitions")
      partitions.toSet shouldBe Set("closing_date", "hour")
      delete(path)
    }

    scenario("directory without partition") {
      Given("table without partition")
      createDir(path)
      createDir(s"$path/_SUCCESS")
      createDir(s"$path/._SUCCESS.crc")
      createFile(s"$path/test1.csv")
      createFile(s"$path/test2.csv")
      createFile(s"$path/test3.csv")
      When("get partitions")
      val partitions = getPartitions(path)
      Then("get the partitions")
      partitions.toSet shouldBe Set()
      delete(path)
    }

    scenario("directory without data") {
      Given("table without partition")
      createDir(path)
      createDir(s"$path/_SUCCESS")
      createDir(s"$path/._SUCCESS.crc")
      When("get partitions")
      val partitions = getPartitions(path)
      Then("get the partitions")
      partitions.toSet shouldBe Set()
      delete(path)
    }

    scenario("directory not exists") {
      Given("directory not exists")
      When("get partitions")
      val partitions = getPartitions(path)
      Then("exception is thrown ")
      partitions.toSet shouldBe Set()
    }
  }

}

