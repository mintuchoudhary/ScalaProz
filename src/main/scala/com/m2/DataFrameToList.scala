package com.m2

import org.apache.spark.sql.SparkSession

object DataFrameToList {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //bin/winutil.exe

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();
    sparkSession.sparkContext.setLogLevel("WARN")

    import sparkSession.implicits._

    val dataframe = Seq(
      Array("123", "abc", "2017", "ABC"),
      Array("456", "def", "2001", "ABC"),
      Array("789", "ghi", "2017", "DEF")).toDF("col")


    val listValue = dataframe.collectAsList()
    println(listValue.get(2)) //[WrappedArray(789, ghi, 2017, DEF)]

   // println(dataframe.asInstanceOf[List])

  }
}
