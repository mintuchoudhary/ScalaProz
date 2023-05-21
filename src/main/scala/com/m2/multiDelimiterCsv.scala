package com.m2

import  org.apache.spark.sql.functions._
import  org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.types._

object multiDelimiterCsv {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Datafame Demo")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "2147480000")
      .master(master = "local[1]")
      .getOrCreate()

      import  spark.implicits._


    val readDF = spark.read.
        option("sep","|")
      //.option("schema",true)
      .csv("E:/IdeaProjects/ScalaProz/src/main/resources/corruptDilimiter.csv")

   // val newDF = readDF.map(_.split("|"))

     val newDF = readDF.toDF("Name","Age")
       .select(regexp_replace(col("Name"),"~"," " ).alias("Name"),col("Age"))
       .filter(" Age !='Age' ")
       .show(false)



  }
}
