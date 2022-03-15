package com.m2.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//ncat.exe -lk 9090 :https://nmap.org/dist/nmap-7.92-setup.exe
// then provide msg for streaming
/**
 * aggregations work at a micro batch level
 * the append output mode not supported without watermarks*
 * some aggregations are not supported e.g sorting or chained aggregation
 */
object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Downloads\\hadoop-common-2.2.0-bin-master\\")

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark Streaming on Dir")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.conf.set("csv.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.streaming.fileSink.log.cleanupDelay", 60000)

    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true),
        StructField("dept", StringType, true)))

    val dStreamData = spark.readStream //format("socket").option("host","localhost").option("port",8090).load()
      .option("header", "true")
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .csv("src/main/resources/stream/")
      .withColumn("timestamp", current_timestamp())

    dStreamData.printSchema()

    val groupDF = dStreamData
      .groupBy("name")
      .agg(max("salary") * lit(5))
    groupDF.printSchema()
//distinct is not supported in aggreagate
    groupDF.writeStream
      .outputMode(OutputMode.Complete()) //append & update are not supported on aggregate without watermarks
      .format("console")
      .option("truncate", "false")
      //.option("newRows", 30)
      .option("checkpointLocation", "c:\\checkpoint")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()

    spark.stop()
  }

}
