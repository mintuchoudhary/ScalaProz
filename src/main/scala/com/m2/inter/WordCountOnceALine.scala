package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.functions._

object WordCountOnceALine {
  def main(args: Array[String]): Unit = {


    val session = utils.getSparkSession()
    import session.implicits._
    val inputDataRdd = session.sparkContext.textFile("src/main/resources/data.txt")
    inputDataRdd.foreach(println)
    val linesHavingWord = inputDataRdd.filter(line => line.contains("Mintu")) //.count()
    println("-------------------------------------------")
    println("Lines having Mintu are::" + linesHavingWord.foreach(println))
    println("wordCountEachLine::" + linesHavingWord.count())

    //Other way to do by dataframe
    println("dataframe count:"+inputDataRdd.toDF("value").filter(col("value").contains("Mintu")).count())
  }
}