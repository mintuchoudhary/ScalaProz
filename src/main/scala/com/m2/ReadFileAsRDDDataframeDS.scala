package com.m2

object ReadFileAsRDDDataframeDS {

  def main(args: Array[String]): Unit = {

    val session = utils.getSparkSession()

    val RDD = session.sparkContext.textFile("src/main/resources/data.txt")
     RDD.foreach(println)

    val dataframe = session.read.text("src/main/resources/data.txt")  //even .load() will give dataframe
    dataframe.show(false)

    val dataset = session.read.textFile("src/main/resources/data.txt")
    dataset.show(false)
  }
}
