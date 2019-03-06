Feature: Integrate with epsilon-hadoop

  Scenario Outline: Test read in differents formats from epsilon
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/epsilon/config_<format>.conf
    When execute in spark
    Then should produce avro output in destPath hdfs://localhost:9000/tests/flow/result/epsilon/epsilon_<format>.avro
    And should contain 25 lines

    Examples:
      | format            |
      | csv               |
      | text              |
      | csvcompress       |
      | textcompress      |
      | avro              |