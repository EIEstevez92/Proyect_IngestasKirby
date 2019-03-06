Feature: Test Flow txt to avro

  As a PO I would like to parse file in txt to Avro with correct format

  Scenario Outline: Test check correct flow
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/<type>/config_<file>.conf
    When execute in spark
    Then should produce <type> output in destPath hdfs://localhost:9000/tests/flow/result/<type>/<file>.<type>
    And should contain <lines> lines
    And check that parse correct schema for <type> in <file>

    Examples:
      | file     | type    | lines |
      | kdat_mrr | avro    | 900   |
      | kdat_paa | avro    | 900   |
      | kdat_mrr | parquet | 900   |
      | kdat_paa | parquet | 900   |
      | fixed    | avro    | 900   |
      | kdat_mrr2| csv     | 900   |


  Scenario Outline: Test check correct flow with two inputs
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/<type>/config_<file>_x2.conf
    When execute in spark
    Then should produce <type> output with double lines in destPath hdfs://localhost:9000/tests/flow/result/<type>/<file>_2.<type>
    And should contain double of <lines> lines

    Examples:
      | file     | type    | lines |
      | kdat_mrr | avro    | 900   |
      | kdat_mrr | parquet | 900   |


  Scenario Outline: Test check correct flow with validate schema
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/validate/config_validate_<validate_type>.conf
    When execute in spark
    Then should produce parquet output in destPath hdfs://localhost:9000/tests/flow/result/parquet/validate_<validate_type>.parquet
    And should contain <lines> lines

    Examples:
      | validate_type | lines |
      | okMaster      | 3     |


  Scenario Outline: Test check flow with invalid matching validation schema
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/validate/config_validate_<validate_type>.conf
    When execute in spark
    Then must exit with <exit_code>

    Examples:
      | validate_type | exit_code |
      | failDataType  | 125        |
      | failNames     | 125        |

  Scenario Outline: Test check correct flow with http schema
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/path/config_<protocol>.conf
    When execute in spark
    Then should produce <type> output in destPath hdfs://localhost:9000/tests/flow/result/path/<type>/<protocol>.<type>
    And should contain <lines> lines
    And check that parse correct schema for <type> in <file>

    Examples:
      | file     | protocol  | type    | lines |
      | kdat_mrr | http      | avro    | 900   |
      | kdat_mrr | hdfs      | avro    | 900   |

  Scenario Outline: Test wrong route
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/legacyName/config_<protocol>.conf
    When execute in spark
    Then should not produce any output in destPath hdfs://localhost:9000/tests/flow/result/path/<protocol>.<type>

    Examples:
      | protocol  | type    |
      | empty     | avro    |
      | wrong     | avro    |

  Scenario Outline: Test originName
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/legacyName/config_<file>_<type>.conf
    When execute in spark
    Then should produce <type> output in destPath hdfs://localhost:9000/tests/flow/result/legacyName/<file>.<type>
    And should contain <lines> lines
    And check that parse correct schema for <type> in <file>

    Examples:
      | file       | type    | lines |
      | legacy     | avro    | 900   |
      | legacy_err | avro    | 900   |
      | legacy     | parquet | 900   |

  Scenario Outline: Test originName error
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/legacyName/config_<file>.conf
    When execute in spark
    Then should not produce any output in destPath hdfs://localhost:9000/tests/flow/result/legacyName/<file>.<type>

    Examples:
      | file       | type    |
      | legacy_err | parquet |