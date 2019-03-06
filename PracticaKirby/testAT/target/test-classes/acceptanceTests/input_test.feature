Feature: Integrate with epsilon-hadoop

  Scenario Outline: Test read in differents formats from epsilon
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/input/config_<format>.conf
    When execute in spark
    Then should produce avro output in destPath hdfs://localhost:9000/tests/flow/result/input/<format>.avro
    And should contain <lines> lines

    Examples:
      | format            | lines |
      | xml               | 12    |
      | textextended     | 138   |