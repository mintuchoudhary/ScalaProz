package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}


object PivotData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Datafame Demo")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "2147480000")
      .master(master = "local[1]")
      .getOrCreate()

     val studDF = spark.read.option("sep","|")
       .option("header","true")
       .option("inferSchema","true")
       .csv("E:/IdeaProjects/ScalaProz/src/main/resources/studentData.csv")

      studDF.show(false)
   // studDF.printSchema()
/*
      val winDF =(Window).partitionBy("ROLL_NO").orderBy("SUBJECT")

    studDF.withColumn("rank",dense_rank().over(winDF)).show()
      filter("rank >1").show() */

      import spark.implicits._

      val studPivotDF = studDF.groupBy("ROLL_NO").pivot("SUBJECT").sum("MARKS")



       studPivotDF.withColumn("total",col("English")+col("History")
        +col("Maths")+col("Physics")+col("Science")).show(false)

     // studDF.show(false)

    spark.close()
  }
}
