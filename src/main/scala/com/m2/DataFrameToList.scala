package com.m2

import org.apache.spark.sql.SparkSession

object DataFrameToList {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //bin/winutil.exe

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();
    sparkSession.sparkContext.setLogLevel("WARN")

    import sparkSession.implicits._

    val dataframe = Seq(
      ("123", "abc", "2017", "ABC"),
      ("456", "def", "2001", "ABC"),
      ("789", "ghi", "2017", "DEF")).toDF("c1", "c2", "c3", "c4")

    dataframe.show()
    /***** List of any 1 cols  *******/
    //approach 1
    println(dataframe.select("c1").collect().map(_ (0)).toList) //List(123, 456, 789)
    //approach 2
    println(dataframe.select("c1").rdd.map(r => r(0)).collect.toList) //List(123, 456, 789)

    //approach 3 - to get list of String
    println(dataframe.select("c1").map(r => r.getString(0)).collect.toList) //List(123, 456, 789)

    println(dataframe.select("c1").as[String].collect.toList)

    /***** List of all cols  *******/
    val listValue = dataframe.collectAsList() //convert entire frame to list
    println(listValue.get(2)) // [789,ghi,2017,DEF]
    println(listValue.get(2).getAs("c1")) // 789
    println(dataframe.select("c1").collectAsList()) //[[123], [456], [789]]

    /***** List of 2 cols  *******/

  }
}
