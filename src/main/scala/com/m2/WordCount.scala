package com.m2

import org.apache.spark.sql.SparkSession

object WordCount {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //for bin/winutils.exe

  def main(args: Array[String]) {
    // val spark = SparkSession.builder().master("local").getOrCreate()
    val sparkSession = SparkSession.builder().appName("word count").master("local").getOrCreate();
      sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._

    // val inputData1 = sparkSession.read.option("header", true).option("delimiter", ",").csv("data.txt")
    val inputData1 = sparkSession.sparkContext.textFile("src/main/resources/data.txt")
    println(inputData1.toDF().show(false))

    val wordCountDF = inputData1.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

   // print(wordCountDF.toDF().show(false))
  //  print(wordCountDF.count)
    wordCountDF.foreach(x => x._1)
    println("end")
    // sparkSession.stop()
  }

}
