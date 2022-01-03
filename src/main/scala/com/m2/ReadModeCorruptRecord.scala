package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * PERMISSIVE - is the default mode, when no mode is specified
 * FAILFAST - will fail if the schema is not matched
 * DROPMALFORMED - will drop the records that does not match the schema
 */
object ReadModeCorruptRecord {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Corrupt File Handler")
      .master("local")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("dept", StringType, true)
    )
    )

    val corruptFileData = spark.read.option("header", "true").schema(schema)
      //.option("mode","PERMISSIVE") // will print the corrupt record as well as normal
      //.option("mode", "FAILFAST") // will give error as : java.lang.RuntimeException: Malformed CSV record
      .option("mode", "DROPMALFORMED") // will print only records matching schema
      .csv("src/main/resources/corruptRecordFile.csv")
    corruptFileData.show(false)
  }
}

/**
 * Input:
 * col1,col2,col3
 * 102,Mintu,IT
 * try again , network issue
 * 203,Meg,IT
 * 404 ,: record not found
 * 103,DD,HR
 * connection failure: server not reachable
 *
 *
 * Output:
 * |id |name |dept|
 * +---+-----+----+
 * |102|Mintu|IT  |
 * |203|Meg  |IT  |
 * |103|DD   |HR  |
 * +---+-----+----+
 */
