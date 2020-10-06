
package com.m2

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.TemporalAdjusters
import java.util.Calendar

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.createDecimalType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.BitSet
import scala.util.Try


object DataFrameExample {
  //case class Person(name: String, age: Int, personid : Int)
  var panelDataFrame1: DataFrame = null
  var panelDataFrame2: DataFrame = null

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DataFrame Example")
    conf.setMaster("local")
    val sparkSession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();
    import sparkSession.implicits._
    val value: String = null


    val ds = Seq(
      Array("123", "abc", "2017", "ABC"),
      Array("456", "def", "2001", "ABC"),
      Array("789", "ghi", "2017", "DEF")).toDF("col")

    val ds2 = Seq(
      Array("123", "abc", "2017", "ABC"),
      Array("456", "def", "2001", "ABC"),
      Array("789", "ghi", "2017", "DEF")).toDF("col")





    println(ds.show)
    println(ds2.show)
  }
}
