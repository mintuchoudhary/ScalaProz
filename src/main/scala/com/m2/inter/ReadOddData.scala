package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Ques : How to read and handle extra column
 * 12,34,56
 * 23,56,86
 * 45,67,58,87
 * 56,77,66
 */
object ReadOddData {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()
    val data = Seq(
      (12, 34, 56),
      (23, 56, 86),
      (45, 67, 58, 87),
      (56, 77, 66)
    )


    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data)
   val schema = StructType(
     List(StructField("c1",StringType,false),
       StructField("c2",StringType,false),
       StructField("c3",StringType,false),
       StructField("c4",StringType,false))
   )
    val dataDF= data.toDF(List("c1","c2","c3","c4"): _*)
dataDF.show()

  }
}
