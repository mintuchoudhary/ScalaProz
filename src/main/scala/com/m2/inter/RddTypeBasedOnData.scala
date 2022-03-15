package com.m2.inter

import com.m2.utils

object RddTypeBasedOnData {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()

    val list = List(1, 2, 4)
    val Stringlist = List("1", "2", "4")
    val ListMap = List(Map("1"->"Onne", "2"->"two"))

    val rdd1 = spark.sparkContext.parallelize(list) //RDD[Int]
    val rdd2 = spark.sparkContext.parallelize(Stringlist) //RDD[String]
    val rdd3 =spark.sparkContext.parallelize(ListMap) //RDD[Map[String,String]]
  }
}
