package com.m2

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
object PassingFunctionAsArgs {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()
    aggregateLogic(sum,spark)
    aggregateLogic(max,spark)
    aggregateLogic(min,spark)
    aggregateLogic(stddev,spark)
    aggregateLogic(avg,spark)
  }

  def aggregateLogic(aggregateFuction :Column=> Column,spark: SparkSession ): Unit = {
    import spark.implicits._
    val inputData = Seq(1,2,3,4).toDF("num")
     inputData.agg(aggregateFuction(col("num"))).show(false)
  }
}
