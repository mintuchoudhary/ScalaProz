package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ReadDelimitedFile {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Read Delimited File")
      .config("spark.testing.memory", "2147480000")
      .master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val fileDF = spark.read.option("header", "true")
      .option("sep", "|")
      .option("inferSchema", "true")
      .format("csv")
      .load("src/main/resources/data.user")
    fileDF.show(false)
    fileDF.printSchema()

    //HOw to select multiple columns based on list of columns
    val colList = List("id", "name", "dept")
    val colNames = colList.map(name => col(name))
    fileDF.select(colNames: _*).show(false)

  }
}
