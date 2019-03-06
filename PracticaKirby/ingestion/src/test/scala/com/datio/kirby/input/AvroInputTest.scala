package com.datio.kirby.input

import java.util.Date

import com.databricks.spark.avro.AvroDataFrameWriter
import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class AvroInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory with FileTestUtil {

  private val goodSchema = """{"type":"record","name":"topLevelRecord","fields":[{"name":"notExists","type":["string","null"], "default": "empty"},{"name":"newDate","type":["string","null"],"logicalFormat":"DATE", "format":"yyyy-MM-dd HH:mm:ss.SSS", "default": "2001-07-04 12:08:56.235"},{"name":"cod_paisoalf","type":["string","null"]},{"name":"cod_entalfa","type":["string","null"]},{"name":"cod_idcontra","type":["string","null"]},{"name":"cod_pgofipro","type":["string","null"]},{"name":"cod_paisfol","type":["string","null"]},{"name":"cod_pgbancsb","type":["string","null"]},{"name":"cod_pgcofici","type":["string","null"]},{"name":"cod_pgccontr","type":["string","null"]},{"name":"cod_folio","type":["string","null"]},{"name":"cod_ctaextno","type":["string","null"]},{"name":"cod_idprodto","type":["string","null"]},{"name":"cod_prodfin","type":["long","null"]},{"name":"cod_situcto","type":["string","null"]},{"name":"cod_situmora","type":["string","null"]},{"name":"cod_situdw","type":["string","null"]},{"name":"cod_diisoalf","type":["string","null"]},{"name":"cod_idiomiso","type":["string","null"]},{"name":"xti_clasecto","type":["string","null"]},{"name":"qnu_firmas","type":["int","null"]},{"name":"xti_admonpg","type":["string","null"]},{"name":"xti_multidiv","type":["string","null"]},{"name":"xti_bloqueo","type":["string","null"]},{"name":"cod_canal_dv","type":["string","null"]},{"name":"cod_matricu","type":["string","null"]},{"name":"cod_pgpaisco","type":["string","null"]},{"name":"cod_pgbancom","type":["string","null"]},{"name":"cod_pgoficom","type":["string","null"]},{"name":"cod_pgofible","type":["string","null"]},{"name":"fec_altapro","type":["string","null"]},{"name":"fec_autopro","type":["string","null"]},{"name":"fec_imprpro","type":["string","null"]},{"name":"fec_altacto","type":["string","null"]},{"name":"fec_vigorcto","type":["string","null"]},{"name":"fec_ultmod","type":["string","null"]},{"name":"fec_vencto","type":["string","null"]},{"name":"fec_cancel","type":["string","null"]},{"name":"cod_tipocor","type":["string","null"]},{"name":"cod_nivconfi","type":["string","null"]},{"name":"xti_tituliza","type":["string","null"]},{"name":"aud_usuario","type":["string","null"]},{"name":"aud_timfmod","type":["string","null"]},{"name":"cod_pgdestin","type":["string","null"]},{"name":"cod_pgsubdes","type":["string","null"]},{"name":"cod_garcoble","type":["string","null"]},{"name":"cod_admonges","type":["string","null"]},{"name":"cod_medioen","type":["int","null"]},{"name":"cod_ccolect","type":["string","null"]},{"name":"cod_paisaud","type":["string","null"]},{"name":"cod_entiaud","type":["string","null"]},{"name":"cod_oficaud","type":["string","null"]},{"name":"cod_medio_dv","type":["string","null"]},{"name":"fec_admonpg","type":["string","null"]},{"name":"hms_admonpg","type":["string","null"]},{"name":"xti_pdte_reg","type":["string","null"]},{"name":"xti_epa", "rename":"xti_epa_renamed" ,"type":["string","null"]}]}"""

  private val goodSchemaWithPartition = """{"type":"record","name":"topLevelRecord","fields":[{"name":"notExists","type":["string","null"], "default": "empty"},{"name":"newDate","type":["string","null"],"logicalFormat":"DATE", "format":"yyyy-MM-dd'T'HH:mm:ss.SSS", "default": "2001-07-04 12:08:56.235"},{"name":"cod_paisoalf","type":["string","null"]},{"name":"cod_entalfa","type":["string","null"]},{"name":"cod_idcontra","type":["string","null"]},{"name":"cod_pgofipro","type":["string","null"]},{"name":"cod_paisfol","type":["string","null"]},{"name":"cod_pgbancsb","type":["string","null"]},{"name":"cod_pgcofici","type":["string","null"]},{"name":"cod_pgccontr","type":["string","null"]},{"name":"cod_folio","type":["string","null"]},{"name":"cod_ctaextno","type":["string","null"]},{"name":"cod_idprodto","type":["string","null"]},{"name":"cod_prodfin","type":["long","null"]},{"name":"cod_situcto","type":["string","null"]},{"name":"cod_situmora","type":["string","null"]},{"name":"cod_situdw","type":["string","null"]},{"name":"cod_diisoalf","type":["string","null"]},{"name":"cod_idiomiso","type":["string","null"]},{"name":"xti_clasecto","type":["string","null"]},{"name":"qnu_firmas","type":["int","null"]},{"name":"xti_admonpg","type":["string","null"]},{"name":"xti_multidiv","type":["string","null"]},{"name":"xti_bloqueo","type":["string","null"]},{"name":"cod_canal_dv","type":["string","null"]},{"name":"cod_matricu","type":["string","null"]},{"name":"cod_pgpaisco","type":["string","null"]},{"name":"cod_pgbancom","type":["string","null"]},{"name":"cod_pgoficom","type":["string","null"]},{"name":"cod_pgofible","type":["string","null"]},{"name":"fec_altapro","type":["string","null"]},{"name":"fec_autopro","type":["string","null"]},{"name":"fec_imprpro","type":["string","null"]},{"name":"fec_altacto","type":["string","null"]},{"name":"fec_vigorcto","type":["string","null"]},{"name":"fec_ultmod","type":["string","null"]},{"name":"fec_vencto","type":["string","null"]},{"name":"fec_cancel","type":["string","null"]},{"name":"cod_tipocor","type":["string","null"]},{"name":"cod_nivconfi","type":["string","null"]},{"name":"xti_tituliza","type":["string","null"]},{"name":"aud_usuario","type":["string","null"]},{"name":"aud_timfmod","type":["string","null"]},{"name":"cod_pgdestin","type":["string","null"]},{"name":"cod_pgsubdes","type":["string","null"]},{"name":"cod_garcoble","type":["string","null"]},{"name":"cod_admonges","type":["string","null"]},{"name":"cod_medioen","type":["int","null"]},{"name":"cod_ccolect","type":["string","null"]},{"name":"cod_paisaud","type":["string","null"]},{"name":"cod_entiaud","type":["string","null"]},{"name":"cod_oficaud","type":["string","null"]},{"name":"cod_medio_dv","type":["string","null"]},{"name":"fec_admonpg","type":["string","null"]},{"name":"hms_admonpg","type":["string","null"]},{"name":"xti_pdte_reg","type":["string","null"]},{"name":"xti_epa","type":["string","null"]},{"name":"closing_date","type":["string","null"]}]}"""

  private val badSchema = """{"type":"record","name":"topLevelRecord","fields":[{"name":"notExists","type":["string"]},{"name":"cod_idcontra","type":["string","null"]},{"name":"cod_pgofipro","type":["string","null"]},{"name":"cod_paisfol","type":["string","null"]},{"name":"cod_pgbancsb","type":["string","null"]},{"name":"cod_pgcofici","type":["string","null"]},{"name":"cod_pgccontr","type":["string","null"]},{"name":"cod_folio","type":["string","null"]},{"name":"cod_ctaextno","type":["string","null"]},{"name":"cod_idprodto","type":["string","null"]},{"name":"cod_prodfin","type":["long","null"]},{"name":"cod_situcto","type":["string","null"]},{"name":"cod_situmora","type":["string","null"]},{"name":"cod_situdw","type":["string","null"]},{"name":"cod_diisoalf","type":["string","null"]},{"name":"cod_idiomiso","type":["string","null"]},{"name":"xti_clasecto","type":["string","null"]},{"name":"qnu_firmas","type":["int","null"]},{"name":"xti_admonpg","type":["string","null"]},{"name":"xti_multidiv","type":["string","null"]},{"name":"xti_bloqueo","type":["string","null"]},{"name":"cod_canal_dv","type":["string","null"]},{"name":"cod_matricu","type":["string","null"]},{"name":"cod_pgpaisco","type":["string","null"]},{"name":"cod_pgbancom","type":["string","null"]},{"name":"cod_pgoficom","type":["string","null"]},{"name":"cod_pgofible","type":["string","null"]},{"name":"fec_altapro","type":["string","null"]},{"name":"fec_autopro","type":["string","null"]},{"name":"fec_imprpro","type":["string","null"]},{"name":"fec_altacto","type":["string","null"]},{"name":"fec_vigorcto","type":["string","null"]},{"name":"fec_ultmod","type":["string","null"]},{"name":"fec_vencto","type":["string","null"]},{"name":"fec_cancel","type":["string","null"]},{"name":"cod_tipocor","type":["string","null"]},{"name":"cod_nivconfi","type":["string","null"]},{"name":"xti_tituliza","type":["string","null"]},{"name":"aud_usuario","type":["string","null"]},{"name":"aud_timfmod","type":["string","null"]},{"name":"cod_pgdestin","type":["string","null"]},{"name":"cod_pgsubdes","type":["string","null"]},{"name":"cod_garcoble","type":["string","null"]},{"name":"cod_admonges","type":["string","null"]},{"name":"cod_medioen","type":["int","null"]},{"name":"cod_ccolect","type":["string","null"]},{"name":"cod_paisaud","type":["string","null"]},{"name":"cod_entiaud","type":["string","null"]},{"name":"cod_oficaud","type":["string","null"]},{"name":"cod_medio_dv","type":["string","null"]},{"name":"fec_admonpg","type":["string","null"]},{"name":"hms_admonpg","type":["string","null"]},{"name":"xti_pdte_reg","type":["string","null"]},{"name":"xti_epa","type":["string","null"]}]}"""


  val PATH_TO_FILE = "path to file"
  feature("check if the files can be readed") {

    scenario("read the file in a valid path") {

      Given(PATH_TO_FILE)
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [
           |      "src/test/resources/configFiles/avro/example_mae.avro"
           |    ]
           |    schema {
           |      path = ""
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")

      val reader = new AvroInputMock(myConfig.getConfig("input"), goodSchema)

      val fields: DataFrame = reader.read(spark)
      val result = fields.collect()

      Then("Return valid rows")
      result.forall(_.getAs[Long]("cod_prodfin").isValidLong) shouldBe true
      result.length shouldBe 4
      fields.schema.length === Schemas.schemaGood.length

      And("Deleted columns have default value")
      result.forall(_.getAs[String]("notExists") === "empty") shouldBe true

      And("Columns with logicalFormat DATE are casted to Date")
      result.forall(_.getAs[Date]("newDate").getTime === 994197600000l) shouldBe true

      And("Columns with rename are renamed")
      result.forall(_.getAs[String]("xti_epa_renamed") != null) shouldBe true

    }

    scenario("read the file in a valid path partitioned") {

      Given(PATH_TO_FILE)
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [
           |      "src/test/resources/configFiles/avro/example_mae_partition.avro"
           |    ]
           |    schema {
           |      path = ""
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), goodSchemaWithPartition)

      val fields: DataFrame = reader.read(spark)

      Then("Return valid row")
      fields.count shouldBe 4

      Then("Partition column schema shouldn't be inferred and match schema pass by config")
      fields.schema.find(_.name=="closing_date").map(_.dataType) shouldBe Some(StringType)

      fields.schema.length === Schemas.schemaGood.length
    }

    scenario("read two files and concatenate in a valid path") {
      Given(PATH_TO_FILE)
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [
           |      "src/test/resources/configFiles/avro/example_mae.avro",
           |      "src/test/resources/configFiles/avro/example_mae.avro"
           |    ]
           |    schema {
           |      path = ""
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), goodSchema)

      val fields: DataFrame = reader.read(spark)

      Then("Not return any valid row")
      fields.count shouldBe 8

      fields.schema.length === Schemas.schemaGood.length
    }

    scenario("Try to read a file with a not valid path") {
      Given(PATH_TO_FILE)
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [
           |      "src/test/resources/configFiles/avro/example_error.avro"
           |    ]
           |    schema {
           |      path = ""
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), goodSchema)

      val thrown: Exception = intercept[Exception] {
        reader.read(spark).count
      }

      Then("there is a null exception")
      assert(thrown.getMessage.nonEmpty)
    }

    scenario("read the file applying a not valid schema") {
      Given(PATH_TO_FILE)
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [
           |      "src/test/resources/configFiles/avro/example_mae.avro"
           |    ]
           |    schema {
           |      path = ""
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), badSchema)

      Then("Cant cast to correct type")
      val e = intercept[SparkException] {
        reader.read(spark).collect()
      }
      e.getCause.getMessage shouldBe "Found topLevelRecord, expecting topLevelRecord, missing required field notExists"
    }

  }

  val path = "src/test/resources/outputTest.avro"

  override def afterEach(): Unit = {
    delete(path)
  }

  feature("Check schema change types") {

    val fieldName = "field"

    scenario("int to string") {
      Given("A collection of data saved in avro")
      import spark.implicits._
      List(1, 2, 3).toDF(fieldName).write.avro(path)

      And("A configuration reading from that data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [ "$path" ]
           |    schema.path = ""
           |  }
        """.stripMargin)

      And("Schema changing the data to string")
      val newSchema = s"""{"type":"record","name":"test","fields":[{"name":"$fieldName","type":"string", "default": "empty"}]}"""

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), newSchema)

      Then("")
      val e = intercept[SparkException] {
        reader.read(spark).collect
      }
      e.getCause.getMessage shouldBe "Found int, expecting string"
    }

    scenario("int to [string,int]") {
      Given("A collection of data saved in avro")
      import spark.implicits._
      List(1, 3, 5).toDF(fieldName).write.avro(path)
      List("2", "4", "6").toDF(fieldName).write.mode("append").avro(path)

      And("A configuration reading from that data")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "avro"
           |    paths = [ "$path" ]
           |    schema.path = ""
           |  }
        """.stripMargin)

      And("Schema changing the data to string")
      val newSchema = s"""{"type":"record","name":"test","fields":[{"name":"$fieldName","type":["string","int"], "default": "empty"}]}"""

      When("Run the reader in path")
      val reader = new AvroInputMock(myConfig.getConfig("input"), newSchema)

      Then("column type are same for all")
      val res = reader.read(spark).collect()
      res.map(_.get(0)).toSet shouldBe Set("1", "2", "3", "4", "5", "6")
    }
  }

  class AvroInputMock(val configM: Config, schemaContent: String) extends AvroInput(configM) {

    override def readJson(path: String): JValue = parse(removeBomCharacter(schemaContent))
  }

}
