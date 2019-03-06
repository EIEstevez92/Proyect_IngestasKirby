Feature: Load Data partitioned by a field

#  As a PO I would like to load file in txt/avro to avro/parquet

  Scenario Outline: Load Data partitioned by a field
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/<type>/config_<file>_part.conf
    When execute in spark
    Then should be stored in hdfs://localhost:9000/tests/flow/result/<type>/<file>_part.<type>
    And should exists files partitioned by <partionedBy>

    Examples:
      | file     | type    | partionedBy                        |
      | kdat_mrr | avro    | fec_ini_proc, fec_cierre           |
      | kdat_mrr | parquet | cod_num_trn, fec_cierre            |


  Scenario Outline: Load Data partitioned by a field overwriting the previous file
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/<type>/config_<file>_overwrite.conf
    And existing a file in hdfs://localhost:9000/tests/flow/result/<type>/<file>_overwrite.<type>
    When execute in spark
    Then should be stored in hdfs://localhost:9000/tests/flow/result/<type>/<file>_overwrite.<type>
    And should exists files partitioned by <partionedBy>

    Examples:
      | file      | type    | partionedBy                        |
      | kdat_mrr  | avro    | fec_ini_proc, fec_cierre           |
      | kdat_mrr  | parquet | cod_num_trn, fec_cierre            |


  Scenario Outline: Load Data partitioned a file while another exist and mode = error default.
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/<type>/config_<file>_part_overwrite.conf
    And existing a file in hdfs://localhost:9000/tests/flow/result/<type>/<file>_part_overwrite.<type>
    When execute in spark
    Then should be stored in hdfs://localhost:9000/tests/flow/result/<type>/<file>_part_overwrite.<type>
    And should not exist this <partionedBy>

    Examples:
      | file     | type    | partionedBy                        |
      | kdat_mrr | avro    | fec_ini_proc, fec_cierre           |
      | kdat_mrr | parquet | value_transaction_date, close_date |

  Scenario Outline: Save a file while another exist and mode = overwrite without force.
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/avro/config_<clue>_overwrite_without_force.conf
    And existing a file in hdfs://localhost:9000/tests/flow/result/avro/<clue>_without_force_overwrite.avro
    When execute in spark
    Then must exit with <exitCode>

    Examples:
      | clue             | exitCode |
      | on_partition     | 3       |
      | on_table         | 4       |

  Scenario Outline: Reprocess a partition.
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/avro/config_kdat_mrr_part.conf
    Given a environment variable with <key> and <value>
    When execute in spark
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/avro/config_<file>.conf
    When execute in spark
    Then must exit with <exitCode>

    Examples:
      | file                                    | key       | value                   | exitCode |
      | with_reprocess                          |           |                         |   0      |
      | with_reprocess_and_env_variables        | REPROCESS | fec_ini_proc=2009-07-03 |   0      |