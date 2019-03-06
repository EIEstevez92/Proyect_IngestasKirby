package com.datio.kirby

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith
import org.junit.{AfterClass, BeforeClass}

import scala.sys.process._

object RunCukesTest {

  @BeforeClass
  def startCompose: Unit = {

    val scriptStartPath = ClassLoader.getSystemResource("docker/start.sh").getPath

    "chmod +x " + scriptStartPath !

    scriptStartPath !
  }

  @AfterClass
  def stopCompose: Unit = {

    val scriptStopPath = ClassLoader.getSystemResource("docker/stop.sh").getPath

    "chmod +x " + scriptStopPath !

    scriptStopPath !
  }

}

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array(
    "src/test/resources/acceptanceTests/api-examples.feature",
    "src/test/resources/acceptanceTests/transformations.feature",
    "src/test/resources/acceptanceTests/flow-txt-avro-parquet.feature",
    "src/test/resources/acceptanceTests/load-file-by-partition.feature",
    "src/test/resources/acceptanceTests/epsilon-integration.feature",
    "src/test/resources/acceptanceTests/check_config.feature",
    "src/test/resources/acceptanceTests/input_test.feature",
    "src/test/resources/acceptanceTests/safe-reprocess.feature",
    "src/test/resources/acceptanceTests/completeFlow.feature"
  ),
  plugin = Array(
    "pretty"
  ))
class RunCukesTest


