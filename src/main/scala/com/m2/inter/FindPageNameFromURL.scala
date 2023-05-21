package com.m2.inter

import com.m2.utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * Ques: Read a file abc.txt in spark having only one column URL. Get page name from URL column
 * (https://myweb.com/dashboard =dashboard, https://yourweb.com/index.jsp = index.jsp)
 *
 */
object FindPageNameFromURL {
  def main(args: Array[String]): Unit = {
    val session = utils.getSparkSession()
    import session.implicits._

    val inputData = session.sparkContext.textFile("src/main/resources/URL.txt")
    println(inputData.toDF("url").show(false))
    println("count: " + inputData.count())
    val res = inputData.map(line => (line, line.lastIndexOf("/")))
      .map(lineWithLastOcc => (lineWithLastOcc, lineWithLastOcc._1.substring(lineWithLastOcc._2 + 1)))
    //.foreach(println)
    println("page Name from URL:" + res.toDF("URL_INPUT", "PAGENAME_OUTPUT").show(false))

    val outData = inputData.toDF("url")
      .withColumn("splitURL", lit(getPageName(col("url"))))
      .withColumn("locateURL", locate("/", reverse(col("url"))))
    //.withColumn("pagename",  substring(col("url"),locate("/", reverse(col("url"))).toString().toInt,5))
    outData.show(false)

    outData.withColumn("contcatenated",
      combineUdf(struct(col("url"), col("splitURL")))).show(false)

  }

  val getPageName = udf((url: String) => {
    println("url:" + url)
    url.substring(url.lastIndexOf("/") + 1)
  })

  def combineUdf: UserDefinedFunction = udf((row: Row) =>
    row.mkString(", ")
  )
}

/**
 * +-----------------------------------+---------+
 * |URL                                |PAGENAME |
 * +-----------------------------------+---------+
 * |https://myweb.com/dashboard       |dashboard|
 * |https://yourweb.com/index.jsp     |index.jsp|
 * +-----------------------------------+---------+
 */
