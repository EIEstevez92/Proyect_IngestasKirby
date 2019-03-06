Feature: Test Flow txt to avro

  As a PO I would like to parse file in txt to Avro with correct format

  Scenario Outline: Test Flow Complete
    Given a directory hdfs://hadoop:9000/tests/flow/completeFlow/<dir> with config, schemas, and staging data
    When execute in spark avro flow
    Then must exit with 0
    When execute in spark parquet flow
    Then must exit with 0
    And should produce parquet output in destPath hdfs://localhost:9000/tests/flow/completeFlow/<outputDir>/parquet
    And should contain <lines> lines

    Examples:
      | dir             | outputDir   | lines |
      | teradata        | teradata    | 11429 |
      | teradataEvolved | teradata    | 22858 |
      | ebou            | ebou        |     6 |
      | fixed_input     | fixed_input |     1 |
      | host            | host        |     5 |
      | oracle          | oracle      |   811 |