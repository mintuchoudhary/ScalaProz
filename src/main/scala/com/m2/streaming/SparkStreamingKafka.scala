package com.m2.streaming

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{explode, split}

/**
 * append =cannot be executed without watermark
 */
object SparkStreamingKafka {
  System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //bin/winutil.exe

  def main(args: Array[String]): Unit = {
    println("streaming data..")

    val spark = SparkSession.builder()
      .appName("Spark Streaming using Kafka")
      .master("local")
       .config("spark.testing.memory", "2147480000")
        .getOrCreate()
   // spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "data_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()
   // val wordCount =  df.flatMap(line => line.).map(word => (word,1)).reduceByKey( _ + _)

    val wordCount = df.select(explode(split(df("value")," ")).alias("word"))
      .groupBy("word").count()
  //  wordCount.show(false)
    println("streaming now"+new Date())
    wordCount.writeStream
      .format("console")
      .outputMode("complete") //complete = publish all msg
      .start() //ERROR StreamMetadata: Error writing stream metadata StreamMetadata
      .awaitTermination()


  }
}