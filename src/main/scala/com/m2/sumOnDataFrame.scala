package com.m2

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object sumOnDataFrame {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop")

  def main(args: Array[String]) {
   val conf = new SparkConf()
   conf.setAppName("DataFrame Example")
   conf.setMaster("local")
   val sparkSession = SparkSession.builder().appName("ConnectingToOracleDatabase")
     .config("spark.testing.memory", "2147480000")
     .master("local").getOrCreate();
   import sparkSession.implicits._


   val input = sparkSession.sparkContext.parallelize(Seq(
     ("a", 5, 7, 9, 12, 13),
     ("b", 6, 4, 3, 20, 17),
     ("c", 4, 9, 4, 6, 9),
     ("d", 1, 2, 6, 8, 1)
   )).toDF("ID", "var1", "var2", "var3", "var4", "var5")

   val columnsToSum = List(col("var1"), col("var2"), col("var3"), col("var4"), col("var5"))

   val output = input.withColumn("sums", columnsToSum.reduce(_ + _))

   output.show()
 }
}

/**
 * Input:
 *
 * +---+----+----+----+----+----+
 * | ID|var1|var2|var3|var4|var5|s
 * +---+----+----+----+----+----+-
 * |  a|   5|   7|   9|  12|  13|
 * |  b|   6|   4|   3|  20|  17|
 * |  c|   4|   9|   4|   6|   9|
 * |  d|   1|   2|   6|   8|   1|
 * +---+----+----+----+----+----+-
 */
