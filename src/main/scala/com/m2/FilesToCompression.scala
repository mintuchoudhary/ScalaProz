package com.m2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * The purpose of this file to to compress hadoop files (supported format: txt, json, csv, xml)
 * It will compress all the files for given hdfs folder and delete the original files.
 * It also supports to uncompress those files by just providing the hdfs folder
 * Command: sh -x hadoop_compress_utility.sh <HDFSPATH> [Optional: <compression type> | none]
 *
 * author: Mintu
 */


object FilesToCompression {

  @transient lazy val LOG: Logger = LogManager.getLogger("FilesToCompression.scala")

  def main(args: Array[String]): Unit = {

    LOG.info("Started FileCompression with parameters:" + args)

    val hdfsPath = args(0)
    var compressType = "gzip"
    if (args.length == 2)
      compressType = args(1).toLowerCase

    //step3:creating spark session
    LOG.info(s"creating spark session for jobType  ")
    val sparkSession = SparkSession.builder()
      .appName("HADOOP-COMPRESSION")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.network.timeout", "30s")
      .config("spark.executor.instances", "1")
      .config("spark.executor.memory", "4G")
      .config("spark.executor.cores", "1")
      .config("spark.driver.memory", "4G")
      .getOrCreate()

    LOG.info(s"HDFS path ::$hdfsPath and compression format: $compressType")
    val hadoopConfig = new Configuration()
    val fs = FileSystem.get(hadoopConfig)
    // GEt list of all files
    val allFiles = fs.listStatus(new Path(s"hdfs://$hdfsPath/"))

    allFiles.foreach(filename => {
      val originalFileName = filename.getPath.toString().split("/").last

      val fileData = sparkSession.read.text(filename.getPath.toString())
      fileData.repartition(1).write.option("compression", compressType).mode(SaveMode.Overwrite).text(s"hdfs://$hdfsPath/gz/")

      //GEt part file name to rename to original file name part-r-0000-.txt.gz
      val partFileName = fs.globStatus(new Path(s"hdfs://$hdfsPath/gz/part*"))(0).getPath().getName()

      //After compression moving file from gz folder to orginal hdfs path

      var movedFlag = false
      if (compressType == "gzip") {
        movedFlag = fs.rename(new Path(s"hdfs://$hdfsPath/gz/$partFileName"), new Path(s"hdfs://$hdfsPath/$originalFileName.gz"))
      }
      else {
        val fileNameNoGz = originalFileName.replace(".gz", "")
        movedFlag = fs.rename(new Path(s"hdfs://$hdfsPath/gz/$partFileName"), new Path(s"hdfs://$hdfsPath/$fileNameNoGz"))
      }

      //Deleting extra folder gz created and original file
      if (movedFlag) {
        fs.delete(new Path(s"hdfs://$hdfsPath/gz"), true)
        fs.delete(new Path(s"hdfs://$hdfsPath/$originalFileName"), true)
      }
    }
    )

  }
}