package com.m2.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextFileStreaming {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop") //bin/winutil.exe
    System.setProperty("spark.driver.allowMultipleContexts", "true")

    val session = SparkSession.builder().appName("TextFileStreaming").master("local")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "2147480000")
      .getOrCreate();

    session.sparkContext.setLogLevel("ERROR")
    val conf = new SparkConf().setMaster("local")
      .set("spark.driver.memory", "471859200")
      .setAppName("NetworkWordCount")

    //  create a StreamingContext, the main entry point for all streaming functionality
    val streamingContext = new StreamingContext(conf, Seconds(2))

    // create a DStream that represents streaming data from a directory source
    val fileDStream = streamingContext.textFileStream("E:/IdeaProjects/ScalaProz/src/main/resources/stream/")
    //TRY  file:///c:/
    // Create a DStream that will connect to hostname:port, like localhost:9999
    // val lines = streamingContext.socketTextStream("localhost", 9999)
    val words = fileDStream.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1));
    val wordCounts = pairs.reduceByKey(_ + _);
    wordCounts.print()
//

    streamingContext.start() //will not start if not written // Start the computation
    streamingContext.awaitTermination() // Wait for the computation to terminate



//
//   val dStreamData = session.readStream.option("maxFilePerTrigger", 1)
//     .text("/src/resources/stream/*")
//
//   dStreamData.writeStream.outputMode("append")
//     .option("truncate", "false")
//     .format("console")
//     .trigger(Trigger.ProcessingTime("5 seconds"))
//     .start()
//     .awaitTermination()
//
//
//  val file = session.readStream.load("/src/main/resources/stream/")
//
//  val query = file.writeStream.format("parquet")
//   .option("checkpointLocation", "/src/main/resources/checkpoint")
//   .start("/src/main/resources/streamed_data/")
//   .awaitTermination()
//

  }
}
