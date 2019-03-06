package com.datio.kirby

import java.sql.Date

import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField, StructType}

package object testUtils {

  case class FilterByFieldTestUser(name: String, weight: Int)

  case class TestEntity(text: String)

  case class TestEntityForPartition(text: String, text2: String = "default", text3: String = "default", text4: String = "default")

  val schemaTestEntityForPartition = StructType(Array(
    StructField("text", StringType, nullable = true, new MetadataBuilder().
      putString("originName", "text").build()),
    StructField("text2", StringType, nullable = true, new MetadataBuilder().
      putString("originName", "text2").build()),
    StructField("text3", StringType, nullable = true, new MetadataBuilder().
      putString("originName", "text3").build()),
    StructField("text4", StringType, nullable = true, new MetadataBuilder().
      putString("originName", "text4").build())))

  case class TestIntEntity(text: Int)

  case class MaskIntegerEntity(fieldToMask: Int)

  case class MaskDoubleEntity(fieldToMask: Double)

  case class MaskLongEntity(fieldToMask: Long)

  case class MaskFloatEntity(fieldToMask: Float)

  case class MaskDateEntity(fieldToMask: Date)

  case class MaskBooleanEntity(fieldToMask: Boolean)

}