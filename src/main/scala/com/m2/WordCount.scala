package com.db

import org.apache.spark.sql.SparkSession

object WordCount {
    def main(args: Array[String]){


      val spark = SparkSession.builder().master("local").getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      val inputData1 = spark.read.option("header",true).option("delimiter",",").csv( "testInput.csv")
      inputData1.show()

      spark.stop()

      println("sdfsa")
    }

  }