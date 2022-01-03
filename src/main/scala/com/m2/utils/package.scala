package com.m2

import org.apache.spark.sql.SparkSession

package object utils {


  def getSparkSession(): SparkSession = {

    val spark = SparkSession.builder().appName("Corrupt File Handler")
      .master("local")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
