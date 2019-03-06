Feature: Test to safe reprocess data

  Scenario Outline: Test check data is saving safely
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/safeReprocess/config_safeReprocessData_<type>.conf
    When execute in spark
    Then should produce <type> output in destPath hdfs://localhost:9000/tests/flow/<type>/<file>.<type>
    And should contain <lines> lines
    Then must exit with <exitCode>

      Examples:
        | file       | type    | lines | exitCode |
        | kdat_mrr2  | parquet | 900   | 0        |
        | kdat_paa   | avro    | 900   | 0        |
        | kdat_mrr_3 | csv     | 900   | 0        |