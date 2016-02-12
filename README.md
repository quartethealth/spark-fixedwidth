# spark-fixedwidth
Fixed-width data source for Spark SQL and DataFrames. Based on (and uses) [databricks-spark-csv](https://github.com/databricks/spark-csv)

## Requirements
This library requires Spark 1.3+ and Scala 2.11+

## Building
Run `sbt assembly` from inside the root directory to generate a JAR

## Running / Using

### In the Spark Shell
`./bin/spark-shell --jars <PATH_TO>/spark-fixedwidth/target/scala-2.11/spark-fixedwidth-assembly-1.0.jar`

### In another project
Add the JAR to your project lib and sbt will include it for you

## Features
This package allows reading fixed-width files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path` (REQUIRED): location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `fixedWidths` (REQUIRED): Int array of the fixed widths of the source file(s)
* `schema`: in [spark SQL form](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType). Otherwise everything is assumed String (unless inferSchema is on)
* `useHeader`: when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is true.
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names
* `inferSchema`: automatically infers column types. It requires one extra pass over the data and is false by default
* `comment`: skip lines beginning with this character. Default is `"#"`. Disable comments by setting this to `null`.
* `mode`: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
  * `PERMISSIVE`: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
  * `DROPMALFORMED`: drops lines which have fewer or more tokens than expected or tokens which do not match the schema
  * `FAILFAST`: aborts with a RuntimeException if encounters any malformed line
* `codec`: compression codec to use when saving to file. Should be the fully qualified name of a class implementing `org.apache.hadoop.io.compress.CompressionCodec` or one of case-insensitive shorten names (`bzip2`, `gzip`, `lz4`, and `snappy`). Defaults to no compression when a codec is not specified.
* `nullValue`: specify a string that indicates a null value. Any fields matching this string will be set as nulls in the DataFrame
* `ignoreLeadingWhiteSpace`: Boolean, default true
* `ignoreTrailingWhiteSpace`: Boolean, default true

### Scala API
__Spark 1.4+:__

See [sample fixed-width files](src/test/resources)
```scala
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", StringType),
    StructField("cost", DoubleType)
))

val sqlContext = new SQLContext(sc)
val fruitWidths = Array(3, 10, 5, 4)
val fruit_resource = 'fruit_fixedwidths.txt'

val result = sqlContext.fixedFile(
    fruit_resource,
    fruitWidths,
    fruitSchema,
    useHeader = false
)
result.show() // Prints top 20 rows in tabular format

// Example without schema, and showing extra options
val fruit_resource = 'fruit_w_headers_fixedwidths.txt'
val result = sqlContext.fixedFile(
    fruit_resource,
    fruitWidths,
    useHeader = true,
    inferSchema = true,
    mode = "DROPMALFORMED",
    comment = '/',
    ignoreLeadingWhiteSpace: true,
    ignoreTrailingWhiteSpace: false,
)
result.collect() // Returns an array that contains all of Rows in this DataFrame
```
