package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id
object PartitionDemo {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //for bin/winutils.exe

  def main(args: Array[String]) {
    // val spark = SparkSession.builder().master("local").getOrCreate()
    val sparkSession = SparkSession.builder().appName("word count")
      .config("spark.testing.memory", "2147480000")
      .master("local").getOrCreate();
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._

   val inputData1 = sparkSession.read.option("header", true).option("delimiter", ",").csv("data.txt")
    val inputData = sparkSession.sparkContext.textFile("src/main/resources/data.txt").toDF() //rdd
    println(inputData.toDF().show(false))

    println("current partition="+inputData.rdd.getNumPartitions)

    val newPartitionData = inputData.repartition(3) //rdd

    println("changed partition="+newPartitionData.rdd.getNumPartitions)

    println("Check which data is in which partition:")
    newPartitionData.toJavaRDD.glom().collect()//.foreach(println)

    newPartitionData.withColumn("partition_id",spark_partition_id()).show(false)
    println("Check partition count in each data")
    newPartitionData.withColumn("partition_id",spark_partition_id()).groupBy ("partition_id").count().show(false)

    sparkSession.stop()
  }

}
