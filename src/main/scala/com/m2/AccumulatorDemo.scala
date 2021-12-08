package com.m2

import org.apache.spark.sql.SparkSession

/**
 * Spark Accumulators are shared variables which are only “added” through an associative
 * and commutative operation and are used to perform counters or sum operations
 * Written by worker executor node and read by driver
 */
object AccumulatorDemo {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("AccumulatorDemo").master("local").getOrCreate();

    val accum = sc.sparkContext.longAccumulator("SumAccumulator")

    sc.sparkContext.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
    println("accumulated value=" + accum.value)
  }
}