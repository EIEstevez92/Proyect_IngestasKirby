package com.datio.kirby

import com.datio.kirby.api.errors._

package object errors {

  val CONFIG_LABEL = "Configuration Error: "
  val CONFIG_READER_LABEL = "Configuration Reader Error: "
  val SPARK_OPTION_LABEL = "Spark Option Reader Error: "
  val CONVERSION_LABEL  = "Conversion Util Error: "
  val SCHEMA_LABEL  = "Schema Validation Error: "
  val JSON_LABEL = "Json Validation Error: "

  //Ingestion Errors. Codes begin at 100
  val CONFIG_READER_PARSING_SCHEMA = KirbyError(
    100,
    CONFIG_READER_LABEL + "Error parsing schema"
  )
  val CONFIG_READER_EMPTY_FILE = KirbyError(
    101,
    CONFIG_READER_LABEL + "The file with path %s is empty"
  )
  val CONFIG_READER_NO_DATA_ROWS = KirbyError(
    102,
    CONFIG_READER_LABEL + "No data rows in the file with path %s"
  )
  val INPUT_CLASS_NOT_IMPLEMENTED = KirbyError(
    103,
    INPUT_LABEL + "Input class %s has not been implemented"
  )
  val OUTPUT_CLASS_NOT_IMPLEMENTED = KirbyError(
    104,
    OUTPUT_LABEL + "Output class %s has not been implemented"
  )
  val TRANSFORMATION_CLASS_NOT_IMPLEMENTED = KirbyError(
    105,
    TRANSFORMATION_LABEL + "Transformation class %s has not been implemented"
  )
  val SPARK_OPTION_WRONG_TYPE = KirbyError(
    106,
    SPARK_OPTION_LABEL + "Config %s have wrong type"
  )
  val INPUT_XMLINPUT_COLUMN_MAPPING_PATH_ERROR = KirbyError(
    107,
    INPUT_LABEL + "XmlInput: Error reading config attribute columnMappingPath"
  )
  val TRANSFORMATION_MASK_FIELD_NOT_EXISTS = KirbyError(
    108,
    TRANSFORMATION_LABEL + "Mask Transformation: Field %s haven't exist to be masked"
  )
  val TRANSFORMATION_TOKENIZER_MODE_ERROR = KirbyError(
    109,
    TRANSFORMATION_LABEL + "Tokenizer: pan mode %s invalid. Allowed modes are %s"
  )
  val CONVERSION_TYPE_ERROR = KirbyError(
    113,
    CONVERSION_LABEL + "Format %s has not been implemented"
  )
  val INPUT_GENERIC_ERROR = KirbyError(
    115,
    INPUT_LABEL + "Fatal error in the Input"
  )
  val TRANSFORMATION_GENERIC_ERROR = KirbyError(
    116,
    TRANSFORMATION_LABEL + "Fatal error in the transformations"
  )
  val OUTPUT_GENERIC_ERROR = KirbyError(
    117,
    OUTPUT_LABEL + "Fatal error in the Output"
  )
  val CONFIGURATION_GENERIC_ERROR = KirbyError(
    118,
    CONFIG_LABEL + "Fatal error in the Configuration"
  )
  val SPARK_OPTION_READ_ERROR = KirbyError(
    119,
    SPARK_OPTION_LABEL + "Error reading spark options for config path %s"
  )
  val CONFIG_OUTPUT_MANDATORY_ERROR = KirbyError(
    120,
    CONFIG_LABEL + "Field %s is mandatory in Output configuration"
  )
  val CONFIG_INPUT_MANDATORY_ERROR = KirbyError(
    121,
    CONFIG_LABEL + "Field %s is mandatory in Input configuration"
  )
  val CONFIG_IO_SCHEMA_ERROR = KirbyError(
    122,
    CONFIG_LABEL + "Schema labels must exist in Kirby configuration file."
  )
  val CONFIG_TRANSFORMATION_MANDATORY_ERROR = KirbyError(
    123,
    CONFIG_LABEL + "Field %s is mandatory in transformation"
  )
  val SCHEMA_DIFF_ERROR = KirbyError(
    125,
    SCHEMA_LABEL + "DataFrame schema doesn't match with validation schema.\n%s"
  )
  val CONFIG_FORMAT_EXCEPTION = KirbyError(
    126,
    CONFIG_LABEL + "Configuration format doesn't pass validations with the next errors:\n %s"
  )
  val CONFIG_CONTENT_EXCEPTION = KirbyError(
    127,
    CONFIG_LABEL + "Configuration content doesn't pass validations with the next errors:\n %s"
  )
  val NO_REPORT_AVAILABLE_ERROR = KirbyError(
    128,
    JSON_LABEL + "No ProcessingReport available"
  )
  val GENERIC_LAUNCHER_ERROR = KirbyError(
    129,
    "KIRBY ERROR: Unchecked exception in one of the steps"
  )
  val SCHEMA_PATH_MANDATORY = KirbyError(
    130,
    SCHEMA_LABEL + "The schema.path is mandatory %s"
  )
}
