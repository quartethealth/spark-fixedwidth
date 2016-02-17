package com.quartethealth.spark.fixedwidth.readers

import com.databricks.spark.csv.readers.{BulkReader, LineReader}
import com.univocity.parsers.fixed.{FixedWidthFieldLengths, FixedWidthParser, FixedWidthParserSettings}

/**
  * Read and parse Fixed-width-like input
  *
  * @param fixedWidths the fixed widths of the fields
  * @param lineSep the delimiter used to separate lines
  * @param commentMarker Ignore lines starting with this char
  * @param ignoreLeadingSpace ignore white space before a field
  * @param ignoreTrailingSpace ignore white space after a field
  * @param headers headers for the columns
  * @param inputBufSize size of buffer to use for parsing input, tune for performance
  * @param maxCols maximum number of columns allowed, for safety against bad inputs
  */
private[readers] abstract class FixedwidthReader(
    fixedWidths: Array[Int],
    lineSep: String = "\n",
    commentMarker: Char = '#',
    ignoreLeadingSpace: Boolean = true,
    ignoreTrailingSpace: Boolean = true,
    headers: Seq[String],
    inputBufSize: Int = 128,
    maxCols: Int = 20480) {
  protected lazy val parser: FixedWidthParser = {
    val settings = new FixedWidthParserSettings(new FixedWidthFieldLengths(fixedWidths: _*))
    val format = settings.getFormat
    format.setLineSeparator(lineSep)
    format.setComment(commentMarker)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingSpace)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingSpace)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(inputBufSize)
    settings.setMaxColumns(maxCols)
    settings.setNullValue("")
    settings.setMaxCharsPerColumn(100000)
    if (headers != null) settings.setHeaders(headers: _*)
    // TODO: configurable?
    settings.setSkipTrailingCharsUntilNewline(true)
    settings.setRecordEndsOnNewline(true)

    new FixedWidthParser(settings)
  }
}

/**
  * Read and parse a single line of Fixed-width-like input. Inefficient for bulk data.
  * @param fixedWidths the fixed widths of the fields
  * @param lineSep the delimiter used to separate lines
  * @param commentMarker Ignore lines starting with this char
  * @param ignoreLeadingSpace ignore white space before a field
  * @param ignoreTrailingSpace ignore white space after a field
  * @param inputBufSize size of buffer to use for parsing input, tune for performance
  * @param maxCols maximum number of columns allowed, for safety against bad inputs
  */
private[fixedwidth] class LineFixedwidthReader(
    fixedWidths: Array[Int],
    lineSep: String = "\n",
    commentMarker: Char = '#',
    ignoreLeadingSpace: Boolean = true,
    ignoreTrailingSpace: Boolean = true,
    inputBufSize: Int = 128,
    maxCols: Int = 20480)
  extends FixedwidthReader(
    fixedWidths,
    lineSep,
    commentMarker,
    ignoreLeadingSpace,
    ignoreTrailingSpace,
    null,
    inputBufSize,
    maxCols)
  with LineReader {
  /**
    * parse a line
    * @param line a String with no newline at the end
    * @return array of strings where each string is a field in the CSV record
    */
  def parseLine(line: String): Array[String] = {
    parser.beginParsing(reader(line))
    val parsed = parser.parseNext()
    parser.stopParsing()
    parsed
  }
}

/**
  * Read and parse Fixed-width-like input
  *
  * @param fixedWidths the fixed widths of the fields
  * @param lineSep the delimiter used to separate lines
  * @param commentMarker Ignore lines starting with this char
  * @param ignoreLeadingSpace ignore white space before a field
  * @param ignoreTrailingSpace ignore white space after a field
  * @param headers headers for the columns
  * @param inputBufSize size of buffer to use for parsing input, tune for performance
  * @param maxCols maximum number of columns allowed, for safety against bad inputs
  */
private[fixedwidth] class BulkFixedwidthReader(
    iter: Iterator[String],
    split: Int,      // for debugging
    fixedWidths: Array[Int],
    lineSep: String = "\n",
    commentMarker: Char = '#',
    ignoreLeadingSpace: Boolean = true,
    ignoreTrailingSpace: Boolean = true,
    headers: Seq[String],
    inputBufSize: Int = 128,
    maxCols: Int = 20480)
  extends FixedwidthReader(
    fixedWidths,
    lineSep,
    commentMarker,
    ignoreLeadingSpace,
    ignoreTrailingSpace,
    headers,
    inputBufSize,
    maxCols
  ) with BulkReader {

  parser.beginParsing(reader(iter))
  private var nextRecord = parser.parseNext()

  /**
    * get the next parsed line.
    *
    * @return array of strings where each string is a field in the fixed-width record
    */
  override def next(): Array[String] = {
    val curRecord = nextRecord
    if(curRecord != null) {
      nextRecord = parser.parseNext()
    } else {
      throw new NoSuchElementException("next record is null")
    }
    curRecord
  }

  override def hasNext: Boolean = nextRecord != null
}
