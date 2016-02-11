package com.quartethealth.spark.fixedwidth

import com.databricks.spark.csv.readers.{BulkReader, LineReader}
import com.quartethealth.spark.fixedwidth.readers.{LineFixedwidthReader, BulkFixedwidthReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv.CsvRelation

class FixedwidthRelation protected[spark] (
    baseRDD: () => RDD[String],
    fixedWidths: Array[Int],
    location: Option[String],
    useHeader: Boolean,
    parseMode: String,
    comment: Character,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType,
    inferSchema: Boolean,
    codec: String = null,
    nullValue: String = "")(@transient override val sqlContext: SQLContext)
  extends CsvRelation(
    baseRDD,
    location,
    useHeader,
    delimiter = '\0',
    quote = null,
    escape = null,
    comment = comment,
    parseMode = parseMode,
    parserLib = "UNIVOCITY",
    ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
    ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
    treatEmptyValuesAsNulls = treatEmptyValuesAsNulls,
    userSchema = userSchema,
    inferCsvSchema = true,
    codec = codec)(sqlContext) {

  protected override def getLineReader(): LineReader = {
    val commentChar: Char = if (comment == null) '\0' else comment

    new LineFixedwidthReader(fixedWidths, commentMarker = commentChar)
  }

  protected override def getBulkReader(header: Seq[String], iter: Iterator[String], split: Int): BulkReader = {
    val commentChar: Char = if (comment == null) '\0' else comment
    new BulkFixedwidthReader(iter, split, fixedWidths,
      headers = header, commentMarker = commentChar)
  }
}
