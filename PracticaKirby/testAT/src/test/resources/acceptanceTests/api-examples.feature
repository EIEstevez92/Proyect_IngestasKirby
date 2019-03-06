Feature: Test Flow with custom implementations

#  As a PO I would like make custom implementations of input, output, transforms and checks

  Scenario: Test custom implementations result OK
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/custom/config_custom.conf
    When execute in spark
    Then should produce custom result in hdfs://localhost:9000/tests/flow/result/custom/output/custom.json



  Scenario Outline: Test custom implementations and count lines
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/custom/config_<file>.conf
    When execute in spark
    Then should produce json output in hdfs://localhost:9000/tests/flow/result/custom/output/<file>.json
    And should contain <lines> lines

    Examples:
      | file                 | lines      |
      | custom_count_lines   | 4          |
