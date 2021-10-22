package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //for bin/winutils.exe

  def main(args: Array[String]) {
    // val spark = SparkSession.builder().master("local").getOrCreate()
    val sparkSession = SparkSession.builder().appName("word count")
      //.config("spark.testing.memory", "2147480000")
      .master("local").getOrCreate();
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._

    // val inputData1 = sparkSession.read.option("header", true).option("delimiter", ",").csv("data.txt")
    val inputData1 = sparkSession.sparkContext.textFile("src/main/resources/data.txt")
    println(inputData1.toDF().show(false))

    val wordCountRDD = inputData1.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordCountRDD.toDF("Word", "Count").show(false)

    val letterCountRDD = inputData1.flatMap(line => line.split("")).map(word => (word, 1)).reduceByKey(_ + _)
    letterCountRDD.toDF("Letter", "Count")
      //.filter(col("Letter") <=> " ")
      .show(50, false)


    sparkSession.stop()
  }

}
