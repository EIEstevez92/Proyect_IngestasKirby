package com.datio.kirby.testUtils

import java.net.URL

import br.eti.kinoshita.testlinkjavaapi.TestLinkAPI
import br.eti.kinoshita.testlinkjavaapi.constants.ExecutionStatus
import br.eti.kinoshita.testlinkjavaapi.model.Build
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

trait InitSparkSessionFunSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  lazy val spark: SparkSession = KirbySuiteContextProvider.getOptSparkSession.get

  var result: Boolean = _
  var api: TestLinkAPI = _
  var TestPlanId: Int = _
  var build: Build = _
  var testCase: String = _

  val MOCK_REPORT = "mockReport"

  override def beforeEach(): Unit = {
    result = false
    testCase = ""
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (KirbySuiteContextProvider._spark != None.orNull &&
      KirbySuiteContextProvider._spark.sparkContext != None.orNull) {

      KirbySuiteContextProvider._spark.sparkContext.stop()
    }
    KirbySuiteContextProvider._spark = None.orNull
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    KirbySuiteContextProvider._spark = SparkSession
      .builder().master("local[*]").appName("Read File").getOrCreate()

    if (Option(System.getenv(MOCK_REPORT)).isDefined && !System.getenv(MOCK_REPORT).toBoolean) {
      //Get Testlink URL
      val url: URL = new URL(System.getenv("url"))

      //Get API
      api = new TestLinkAPI(url, System.getenv("devkey"))

      //Get TestPlan
      TestPlanId = api.getTestPlanByName(System.getenv("testPlan"), System.getenv("testProject")).getId

      //Get or Create Build
      api.createBuild(TestPlanId, System.getenv("build"), None.orNull)
      build = api.getLatestBuildForTestPlan(TestPlanId)
    }
  }

  override def afterEach(): Unit = {
    if (testCase != "") {
      updateTestLinkResult(testCase, result)
    }
  }

  def updateTestLinkResult(testCase: String, result: Boolean) {

    if (Option(System.getenv(MOCK_REPORT)).isDefined && !System.getenv(MOCK_REPORT).toBoolean) {
      //Get Result
      val exeResult: ExecutionStatus = if (result) ExecutionStatus.PASSED else ExecutionStatus.FAILED

      //Get TestCase
      val tc = api.getTestCaseByExternalId(testCase, None.orNull)

      //Update Test Case result
      api.reportTCResult(
        tc.getId,
        None.orNull,
        TestPlanId,
        exeResult,
        build.getId,
        None.orNull,
        "Ejecución automática",
        None.orNull,
        None.orNull,
        None.orNull,
        None.orNull,
        None.orNull,
        true)
    }

  }
}




