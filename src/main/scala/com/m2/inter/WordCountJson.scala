package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WordCountJson {

  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()
    import spark.implicits._
    val schema = StructType(List(StructField("name", StringType, false)))


    val rdd = spark.sparkContext.wholeTextFiles("src/main/resources/stock.json").values
    rdd.foreach(println)
    val toSeq = rdd.flatMap(line => line.split(",")).map(pair => pair.split(":").last)
      .map(value => value.trim.replace("\"", "").replace("}", "")).collect().toSeq
    val jsonData = spark.sparkContext.parallelize(toSeq).toDF("name").filter(col("name").isNotNull)
    jsonData.show(false)
    //Word count logic on DataFrame
    jsonData.groupBy("name").count().show(false)

    import spark.implicits._
//    val df3 = jsonData.map(row=>{
//
//      val fullName = row.getString(0) +row.getString(1) +row.getString(2)
//      (fullName, row.getString(3),row.getInt(5))
//    })


    val data = spark.read.option("multiline", "true").json(spark.sparkContext.wholeTextFiles("src/main/resources/stock.json").values)
    data.show(false)
    // val df = spark.read.option("multiline","true").option("inferSchema","true").json("src/main/resources/stock.json")
    //df.show(false)

  }
}
