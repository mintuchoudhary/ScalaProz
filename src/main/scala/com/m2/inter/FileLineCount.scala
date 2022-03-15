package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.SparkSession

/**
 * Ques: Write Spark program to count lines in a textfile
 */

object FileLineCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = utils.getSparkSession()
    import sparkSession.implicits._

    // val inputData2 = sparkSession.read.option("header", true).option("delimiter", ",").csv("data.txt")
    val inputData1 = sparkSession.sparkContext.textFile("src/main/resources/data.txt")
    println(inputData1.toDF().show(false))
    println("count: "+inputData1.count())

    val fileDF = sparkSession.read.option("header", "true")
      .option("sep", "|")
      .option("inferSchema", "true")
      .format("csv")
      .load("src/main/resources/data.user")
    fileDF.show(false)
    println("count csv: "+inputData1.count())
  }
}
