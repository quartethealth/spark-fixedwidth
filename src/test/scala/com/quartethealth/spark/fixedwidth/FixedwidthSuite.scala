package com.quartethealth.spark.fixedwidth

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class FixedwidthSuite extends FunSuite with BeforeAndAfterAll {
  protected def fruit_resource(name: String = ""): String =
    s"src/test/resources/fruit_${name}_fixedwidth.txt"

  protected val fruitWidths = Array(3, 10, 5, 4)
  protected val fruitSize = 7
  protected val fruitFirstRow = Seq(56, "apple", "TRUE", 0.56)

  protected val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", StringType),
    StructField("cost", DoubleType)
  ))

  protected var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "FixedwidthSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  protected def sanityChecks(resultSet: DataFrame): Unit = {
    resultSet.show()
    assert(resultSet.collect().length === fruitSize)

    val head = resultSet.head()
    assert(head.length === fruitWidths.length)
    assert(head.toSeq === fruitFirstRow)
  }

  test("Parse basic") {
    val result = sqlContext.fixedFile(fruit_resource(), fruitWidths, fruitSchema,
      useHeader = false)
    sanityChecks(result)
  }

  test("Parse with headers, ignore") {
    val result = sqlContext.fixedFile(fruit_resource("w_headers"), fruitWidths,
      fruitSchema, useHeader = true)
    sanityChecks(result)
  }

  test("Parse with overflow, ignore") {
    val result = sqlContext.fixedFile(fruit_resource("overflow"), fruitWidths,
      fruitSchema, useHeader = false)
    sanityChecks(result)
  }

  test("Parse with underflow, ignore") {
    val result = sqlContext.fixedFile(fruit_resource("underflow"), fruitWidths,
      fruitSchema, useHeader = false)
    sanityChecks(result)
  }

  test("Parse basic without schema, uninferred") {
    val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
      useHeader = false, inferSchema = false)
    sanityChecks(result)
  }

  test("Parse basic without schema, inferred") {
    val result = sqlContext.fixedFile(fruit_resource(), fruitWidths,
      useHeader = false, inferSchema = true)
    sanityChecks(result)
  }

  test("Parse with headers without schema, inferred") {
    val result = sqlContext.fixedFile(fruit_resource("w_headers"), fruitWidths,
      useHeader = true, inferSchema = true)
    sanityChecks(result)
  }

  test("Parse with comments, ignore") {
    val result = sqlContext.fixedFile(fruit_resource("comments"), fruitWidths,
      useHeader = true, inferSchema = true, comment = '/')
    sanityChecks(result)
  }

  test("Parse malformed, schemaless, PERMISSIVE") {
    val result = sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
      useHeader = false, mode = "PERMISSIVE")
    result.show()
    assert(result.collect().length === fruitSize)
  }

  test("Parse malformed, schemaless, DROPMALFORMED") {
    val result = sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
      useHeader = false, mode = "DROPMALFORMED")
    result.show()
    assert(result.collect().length < fruitSize)
  }

  test("Parse malformed, with schema, FAILFAST") {
    intercept[Exception](
      sqlContext.fixedFile(fruit_resource("malformed"), fruitWidths,
        fruitSchema, useHeader = false, mode = "FAILFAST").collect()
    )
  }

}
