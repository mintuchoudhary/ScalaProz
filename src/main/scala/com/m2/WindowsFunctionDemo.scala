package com.m2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowsFunctionDemo {
  def main(args: Array[String]): Unit = {
    val simpleData = Seq(("Akash", "IT", 3000),
      ("Mintu", "IT", 4600),
      ("Sandy", "IT", 4100),
      ("Maria", "Finance", 3000),
      ("Mga", "IT", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Dee", "IT", 4100)
    )
    val spark = SparkSession.builder().appName("Windows Functions").master("local").config("spark.testing.memory", "2147480000").getOrCreate()
    import spark.implicits._
    val dataDF = simpleData.toDF("employee_name", "department", "salary")
    dataDF.show()

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    dataDF.withColumn("row_number",row_number().over(windowSpec)).show(false)
    dataDF.withColumn("rank",rank().over(windowSpec)).show(false)
    dataDF.withColumn("dense_rank",dense_rank().over(windowSpec)).show(false)
    //lag
    dataDF.withColumn("lag",lag("salary",2).over(windowSpec)).show(false)
    dataDF.withColumn("lead",lead("salary",2).over(windowSpec)).show(false)
  }
}

/**
 * +-------------+----------+------+----------+
 * |employee_name|department|salary|row_number|
 * +-------------+----------+------+----------+
 * |Maria        |Finance   |3000  |1         |
 * |Scott        |Finance   |3300  |2         |
 * |Jen          |Finance   |3900  |3         |
 * |Kumar        |Marketing |2000  |1         |
 * |Jeff         |Marketing |3000  |2         |
 * |Akash        |IT        |3000  |1         |
 * |Mga        |IT        |3000  |2         |
 * |Sandy        |IT        |4100  |3         |
 * |Dee          |IT        |4100  |4         |
 * |Mintu        |IT        |4600  |5         |
 * +-------------+----------+------+----------+
 *
 * +-------------+----------+------+----+
 * |employee_name|department|salary|rank|
 * +-------------+----------+------+----+
 * |Maria        |Finance   |3000  |1   |
 * |Scott        |Finance   |3300  |2   |
 * |Jen          |Finance   |3900  |3   |
 * |Kumar        |Marketing |2000  |1   |
 * |Jeff         |Marketing |3000  |2   |
 * |Akash        |IT        |3000  |1   |
 * |Mga        |IT        |3000  |1   |
 * |Sandy        |IT        |4100  |3   |
 * |Dee          |IT        |4100  |3   |
 * |Mintu        |IT        |4600  |5   |
 * +-------------+----------+------+----+
 *
 * +-------------+----------+------+----------+
 * |employee_name|department|salary|dense_rank|
 * +-------------+----------+------+----------+
 * |Maria        |Finance   |3000  |1         |
 * |Scott        |Finance   |3300  |2         |
 * |Jen          |Finance   |3900  |3         |
 * |Kumar        |Marketing |2000  |1         |
 * |Jeff         |Marketing |3000  |2         |
 * |Akash        |IT        |3000  |1         |
 * |Mga        |IT        |3000  |1         |
 * |Sandy        |IT        |4100  |2         |
 * |Dee          |IT        |4100  |2         |
 * |Mintu        |IT        |4600  |3         |
 * +-------------+----------+------+----------+
 *
 * +-------------+----------+------+----+
 * |employee_name|department|salary|lag |
 * +-------------+----------+------+----+
 * |Maria        |Finance   |3000  |null|
 * |Scott        |Finance   |3300  |null|
 * |Jen          |Finance   |3900  |3000|
 * |Kumar        |Marketing |2000  |null|
 * |Jeff         |Marketing |3000  |null|
 * |Akash        |IT        |3000  |null|
 * |Mga        |IT        |3000  |null|
 * |Sandy        |IT        |4100  |3000|
 * |Dee          |IT        |4100  |3000|
 * |Mintu        |IT        |4600  |4100|
 * +-------------+----------+------+----+
 *
 * +-------------+----------+------+----+
 * |employee_name|department|salary|lead|
 * +-------------+----------+------+----+
 * |Maria        |Finance   |3000  |3900|
 * |Scott        |Finance   |3300  |null|
 * |Jen          |Finance   |3900  |null|
 * |Kumar        |Marketing |2000  |null|
 * |Jeff         |Marketing |3000  |null|
 * |Akash        |IT        |3000  |4100|
 * |Mga        |IT        |3000  |4100|
 * |Sandy        |IT        |4100  |4600|
 * |Dee          |IT        |4100  |null|
 * |Mintu        |IT        |4600  |null|
 * +-------------+----------+------+----+
 */
