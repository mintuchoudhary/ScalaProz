package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.functions._

object WordCountOnDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()

    val inputData = spark.read.text("src/main/resources/data.txt") //dataframe
    println(inputData.show(false))
    val splitData = inputData.select(split(col("value"), " ")).as("words")
    splitData.show(false)
    val wordCountDF = splitData.select(explode(col("words"))).as("word").groupBy("word").count()
    wordCountDF.show(false)
  }
}
