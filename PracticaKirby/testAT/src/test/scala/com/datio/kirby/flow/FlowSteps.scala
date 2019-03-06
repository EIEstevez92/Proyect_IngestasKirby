package com.datio.kirby.flow

import com.datio.kirby.exec.ExecConfig
import com.datio.kirby.flow.masterization._
import com.datio.kirby.read._
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.scalatest.Matchers

class FlowSteps extends Matchers with ScalaDsl with EN {

  var configFilePathHDFS = ""
  var completeFlowDirPathHDFS = ""
  var envVariables: Map[String, String] = Map()

  var df: DataFrame = _
  var linesCompleted = 0L
  var FINAL_LINES_COUNT = "finalLinesCount"
  var RESULT_INGEST = "resultIngest"
  var exit_code: Int = _

  Given("""^a directory (.*) with config, schemas, and staging data$""") {
    (completeFlowDirPathHDFS: String) => this.completeFlowDirPathHDFS = completeFlowDirPathHDFS
  }

  When("""^execute in spark avro flow$"""){
    val configFilePathHDFS = s"$completeFlowDirPathHDFS/avro.conf"
    exit_code = ExecConfig.main(configFilePathHDFS, envVariables)
  }

  When("""^execute in spark parquet flow$"""){
    val configFilePathHDFS = s"$completeFlowDirPathHDFS/parquet.conf"
    exit_code = ExecConfig.main(configFilePathHDFS, envVariables)
  }

  Given("""^a config file path in HDFS (.*)$""") {
    (configFilePathHDFS: String) =>

      this.configFilePathHDFS = configFilePathHDFS

  }

  Given("""^a environment variable with (.*) and (.*)$""") {
    (key: String, value: String) =>

      envVariables = envVariables + (key -> value)

  }

  When("""^execute in spark$""") {
    () =>
      exit_code = ExecConfig.main(configFilePathHDFS, envVariables)
  }


  Then("""^must exit with (\d+)$""") {
    (exitCheck: Int) =>
      exit_code shouldBe exitCheck
  }

  Then("""^should produce avro output in destPath (.*)$""") {
    (pathOutput: String) =>

      df = AvroReader.read(pathOutput)

  }


  Then("""^should produce csv output in destPath (.*)$""") {
    (pathOutput: String) =>

      df = CsvReader.read(pathOutput)

  }

  Then("""^should produce parquet output in destPath (.*)$""") {
    (pathOutput: String) =>

      df = ParquetReader.read(pathOutput)

  }

  Then("""^should produce json output in (.*)$""") {
    (pathOutput: String) =>

      df = JSONReader.read(pathOutput)

  }

  Then("""^should not produce any output in destPath (.*)$""") {
    (pathOutput: String) =>

      assert(!CheckPathReader.check(pathOutput))
  }

  And("""^should contain (\d+) lines$""") {
    (lines: Int) =>

      linesCompleted = df.count()

      linesCompleted shouldBe lines
  }

  And("""^check that parse correct schema for (.*) in (.*)$""") {
    (typeFile: String, file: String) =>
      if (typeFile !== "csv") {
        val dfSchema: Array[StructField] = df.schema.fields

        val schemaToCompare: Array[StructField] = SchemasFlow.getSchema(typeFile, file).fields

        val dfSchemaWithoutMetadata = dfSchema.map(fieldWithMetadata =>
          StructField(fieldWithMetadata.name, fieldWithMetadata.dataType, fieldWithMetadata.nullable)
        )

        typeFile match {
          case "csv" =>
            assert(dfSchemaWithoutMetadata.length == schemaToCompare.length,
              "different array size between resultSchema and checkSchema")

          case _ => checkSchemas(dfSchemaWithoutMetadata, schemaToCompare)
        }
      }
  }

  And("""^without duplicates in columns with (.*)""") {
    (columnsAffected: String) =>

      if (columnsAffected !== "nothing") {
        val columns = columnsAffected match {
          case "primaryKeys" => Seq("field_1", "field_2")
          case "allColumns" => Seq("field_1", "field_2", "field_3", "field_4")
        }
        assert(df.count() === df.dropDuplicates(columns).count())
      }

  }


  And("""should contain (\d+) columns called (.*)""") {
    (columns: Int, columnsName: String) =>

      val columnsMatcher = columnsName.split(",")

      assert(
        columnsMatcher.forall(
          df.columns.contains(_)
        )
      )

      df.columns.length shouldBe columns
  }


  And("""result is correct for transformation (.*)""") {
    (transformation: String) => {

      transformation.toLowerCase match {
        case "initnulls" => InitNullsTestAC.check(df)
        case "catalog" => CatalogTestAC.check(df)
        case "integrity" => IntegrityTestAC.check(df)
        case "literal" => LiteralTestAC.check(df)
        case "mask" => MaskTestAC.check(df)
        case "partialinfo" => PartialInfoTestAC.check(df)
        case "token" => TokenTestAC.check(df)
        case "dateformatter" => DateFormatterTestAC.check(df)
        case "copycolumn" => CopyColumnTestAC.check(df)
        case "extractinfofromdate" => ExtractInfoFromDateTestAC.check(df)
        case "replace" => ReplaceTestAC.check(df)
        case "formatter" => FormatterTestAC.check(df)
        case "leftpadding" => LeftPaddingTestAC.check(df)
        case "trim" => TrimTestAC.check(df)
        case "mixed" => MixedTestAC.check(df)
        case "updatetime" => UpdateTimeTestAC.check(df)
        case "sqlfilter" => SqlFilterTestAC.check(df)
        case "join" => JoinTestAC.check(df)
        case "join2" => JoinTestAC.check2(df)
        case "join3" => JoinTestAC.check3(df)
        case "regexcolumn" => JoinTestAC.check4(df)

      }

    }
  }

  private def checkSchemas(resultSchema: Seq[StructField], checkSchema: Seq[StructField]): Unit = {
    if (resultSchema.isEmpty) {
      if (checkSchema.nonEmpty) {
        fail("different array size between resultSchema and checkSchema")
      }
    } else {
      assert(resultSchema.head == checkSchema.head, s"field ${resultSchema.head.name} is not equals in result and check schema: \n" +
        s"${resultSchema.map(sf => "StructField(\"" + sf.name + "\", " + sf.dataType.toString + ", nullable = " + sf.nullable + ")").mkString(",\n")}")
      checkSchemas(resultSchema.tail, checkSchema.tail)
    }
  }

}