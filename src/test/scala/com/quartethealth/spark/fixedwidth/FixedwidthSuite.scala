package com.quartethealth.spark.fixedwidth


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FixedwidthSuite extends FunSuite with BeforeAndAfterAll {
  def fruit_resource(name: String = ""): String = s"src/test/resources/fruit_${name}_fixedwidth.txt"

  val fruitWidths = Array(3, 10, 5, 4)
  val fruitSize = 7
  val fruitFirstRow = Seq(56, "apple", "TRUE", 0.56)

  val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", StringType),
    StructField("cost", DoubleType)
  ))

  private var sqlContext: SQLContext = _

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

  private def sanityChecks(resultSet: DataFrame): Unit = {
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

}
