Feature: Check config validation with error files

#  As a PO I would like make custom implementations of input, output, transforms, checks and metrics customs

  Scenario Outline: Config with error should raise an exception
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/checkError/config_<file>.conf
    When execute in spark
    Then must exit with <exit_code>

    Examples:
      | file       | exit_code |
      | missing    | 126        |
      | not_exists | 255        |