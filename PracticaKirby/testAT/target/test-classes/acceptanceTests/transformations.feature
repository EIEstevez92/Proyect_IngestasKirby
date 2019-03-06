Feature: Test transformations


  Scenario Outline: Test cleanNulls and dropDuplicates transformations
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/transformations/config_<file>_<result>_<columnsAffected>.conf
    When execute in spark
    Then should produce avro output in destPath hdfs://localhost:9000/tests/flow/result/avro/<file>_<result>_<columnsAffected>.avro
    And should contain <lines> lines
    And without duplicates in columns with <columnsAffected>

    Examples:
      | file                         | lines | result           | columnsAffected |
      | cleanNulls_dropDuplicates | 4      | OK               | primaryKeys      |
      | cleanNulls_dropDuplicates | 5      | OK               | allColumns       |
      | cleanNulls_dropDuplicates | 7      | without_checks  | nothing          |


  Scenario Outline: Test dropColumns, renameColumns and selectColumns transformations
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/transformations/config_<type>.conf
    When execute in spark
    Then should produce avro output in destPath hdfs://localhost:9000/tests/flow/result/avro/<type>.avro
    And should contain <columns> columns called <columnsNames>

    Examples:
      | type                  | columns | columnsNames                                    |
      | dropColumns_two      | 2       | field_1,field_2                                 |
      | dropColumns_empty    | 4       | field_1,field_2,field_3,field_4               |
      | dropColumns_notExist | 4       | field_1,field_2,field_3,field_4               |
      | renameColumns         | 4       | field_1,field2Renamed,field_3,field4Renamed |
      | selectColumns         | 2       | field_1,field_3                               |


  Scenario Outline: Test filterByField transformations
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/transformations/config_filter_<type>.conf
    When execute in spark
    Then should produce avro output in destPath hdfs://localhost:9000/tests/flow/result/avro/filter_<type>.avro
    And should contain <lines> lines

    Examples:
      | type      | lines   |
      | eq        | 232     |
      | neq       | 668     |
      | lt        | 101     |
      | leq       | 333     |
      | gt        | 567     |
      | geq       | 799     |
      | and       | 668     |
      | or        | 900     |


  Scenario Outline: Test initNulls, integrity, catalog, literal, mask, partialInfo, token, dateFormatter, copyColumn,
  toAlphanumeric, extractInfoFromDate, replace, formatter, trim, mixed, regex and updateTime transformations
    Given a config file path in HDFS hdfs://hadoop:9000/tests/flow/config/transformations/config_<transformation>.conf
    When execute in spark
    Then should produce parquet output in destPath hdfs://localhost:9000/tests/flow/result/parquet/<transformation>.parquet
    And result is correct for transformation <transformation>

    Examples:
      | transformation       |
      | initNulls            |
      | integrity            |
      | catalog              |
      | literal              |
      | mask                 |
      | partialInfo          |
      | token                |
      | dateFormatter        |
      | copyColumn           |
      | leftPadding          |
      | extractInfoFromDate  |
      | replace              |
      | formatter            |
      | trim                 |
      | mixed                |
      | updateTime           |
      | sqlFilter            |
      | join                 |
      | join2                |
      | join3                |
      | regexColumn          |