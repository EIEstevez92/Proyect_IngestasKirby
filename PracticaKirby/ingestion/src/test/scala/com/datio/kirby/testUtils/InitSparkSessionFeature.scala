package com.datio.kirby.testUtils

import java.net.URL

import br.eti.kinoshita.testlinkjavaapi.TestLinkAPI
import br.eti.kinoshita.testlinkjavaapi.constants.ExecutionStatus
import br.eti.kinoshita.testlinkjavaapi.model.Build
import com.datio.kirby.constants.TestLiterals._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec}

trait InitSparkSessionFeature extends FeatureSpec with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit lazy val spark: SparkSession = KirbySuiteContextProvider.getOptSparkSession.get
  var result: Boolean = _
  var api: TestLinkAPI = _
  var TestPlanId: Int = _
  var build: Build = _
  var testCase: String = _

  override def beforeEach(): Unit = {
    result = false
    testCase = ""
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (KirbySuiteContextProvider.getOptSparkContext.isDefined) {
      KirbySuiteContextProvider.getOptSparkContext.get.stop()
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val sConf = new SparkConf().setMaster("local[*]").setAppName("Read File")

    sys.props.put("tokenization_app_name", "cloud")
    sys.props.put("tokenization_app_pass", "eG73ao2adAkZQl1PmYg")
    sys.props.put("tokenization_app_endpoint", "LOCAL")


    SparkContext.getOrCreate(sConf)
    KirbySuiteContextProvider._spark = SparkSession
      .builder().master("local[*]").appName("Read File").getOrCreate()

    if (Option(System.getenv(MOCK_REPORT_LIT)).isDefined && !System.getenv(MOCK_REPORT_LIT).toBoolean) {
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

    if (Option(System.getenv(MOCK_REPORT_LIT)).isDefined && !System.getenv(MOCK_REPORT_LIT).toBoolean) {
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
