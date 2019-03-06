package com.datio.kirby.config.validator

import java.io.File
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util

import com.datio.kirby.constants.ConfigConstants.APPLICATION_CONF_SCHEMA
import com.github.fge.jsonschema.core.report.LogLevel
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigValidatorAllTest extends FeatureSpec
  with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  feature("Configs") {
    val successTests = new SuccessConfigTestGenerator()
    for (testScenario <- successTests.scenarios) {
      scenario(testScenario.name) {
        testScenario.run()
      }
    }
  }
}

trait TestCase[T] {
  def name: String

  def data: T

  def run(): Unit
}

object TestCase {
  lazy val schema = InputResource(APPLICATION_CONF_SCHEMA)
  lazy val successConfigHomePath: Path = Paths.get("src/test/resources/config")
}

case class TestSuccessConfig(path: Path, data: File) extends TestCase[File] {
  override def name: String = s"OK - ${path.toString}"

  override def run(): Unit = {
    val errorMessages = ConfigValidator.validate(InputFile(data), TestCase.schema, LogLevel.ERROR)
    assert(errorMessages.isEmpty)
  }
}

case class TestErrorConfig(path: Path, data: File) extends TestCase[File] {
  override def name: String = s"ERROR - ${path.toString}"

  override def run(): Unit = {
    val errorMessages = ConfigValidator.validate(InputFile(data), TestCase.schema, LogLevel.ERROR)
    assert(errorMessages.nonEmpty)
  }
}

trait TestGenerator[T] {
  def scenarios: List[T]
}

class SuccessConfigTestGenerator extends TestGenerator[TestSuccessConfig] {
  override def scenarios: List[TestSuccessConfig] = {
    val file = new File(TestCase.successConfigHomePath.toString)
    val options = util.EnumSet.of(FileVisitOption.FOLLOW_LINKS)
    Files.walkFileTree(file.toPath, options, Integer.MAX_VALUE, VisitorSuccessConfigs)
    VisitorSuccessConfigs.tests
  }
}

object VisitorSuccessConfigs extends VisitorSuccessConfigs

class VisitorSuccessConfigs extends SimpleFileVisitor[Path] {

  private val resourcesExcludes = List("config_missing.conf","config_with_reprocess_and_env_variables.conf")
  var tests: List[TestSuccessConfig] = List()

  override def visitFile(t: Path, basicFileAttributes: BasicFileAttributes): FileVisitResult = {
    val file = new File(t.toString)
    if (file.isFile && !resourcesExcludes.contains(getSimpleName(file))) {
      tests = TestSuccessConfig(t, file) :: tests
    }
    FileVisitResult.CONTINUE
  }

  private def getSimpleName(file: File): String = {
    val nameSplitted = file.getName.split(File.separator)
    nameSplitted(nameSplitted.length - 1)
  }

}