package com.db

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.{Success, Try}
case class Student(val name: String, val age: Int) {
 def this( name: String) {
    this(name,100)
  }

  def print() {
    println(name + " " + age)
  }

}

object DBConnectSparkSession2 {


  def main(args: Array[String]) {

     println("Trying to connect to database... ")
     val sparksession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();


        val connectionProperties = new Properties()
         connectionProperties.put("user", ConfigFactory.load().getString("USER_NAME"))
         connectionProperties.put("password", ConfigFactory.load().getString("PASSWORD"))
         connectionProperties.put("driver", ConfigFactory.load().getString("DRIVER"))

         val eventData = sparksession.read.jdbc(ConfigFactory.load().getString("ORACLE_URL"),
           "T_XA_TITAN_UPSTREAM_EVENT",connectionProperties);


         eventData.printSchema()
         eventData.show()

    import sparksession.implicits._
    val outdoorgamesDF = Seq(
      (8, "cricket"),
      (9, "running"),
      (-7, "swimming")
    ).toDF("number", "name")

    val dataMap = Map.newBuilder[String, DataFrame]

    Try({
    dataMap.+=("T_XA_TITAN_UPSTREAM_EVENT" -> eventData)
    println(dataMap)
    }) match {
      case Success(s) => print(1)
      case _ => None
    }
    //val df =  sparksession.sql("SELECT * FROM T_XA_TITAN_UPSTREAM_MSG WHERE LOAD_DATE_D=TRUNC(SYSDATE)")
    //df.show()

    // Create Sample Dataframe
    val empDF = sparksession.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    val partitionWindow = Window.partitionBy(col("deptno")).orderBy("sal")

    println(dense_rank.over(partitionWindow))
    println(dense_rank().over(partitionWindow))
    println(dense_rank.over(partitionWindow).desc)
    println(rank().over(partitionWindow))
    sparksession.close()
  }


}
