package com.db

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object DBConnect {

  def main(args: Array[String]) {
 /*   val conf = new SparkConf();
    conf.setAppName("ConnectingToOracleDatabase")
    conf.setMaster("local")
    val sc = new SparkContext(conf);
    //sc.setLogLevel("INFO")
    val sqlContext = new SQLContext(sc)        //          dtsrgfdgs

    println("Trying to connect to database... ")*/
    //val sparksession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();

  /*  val eventData = sqlContext.read.format("jdbc")
      .option("url", ConfigFactory.load().getString("ORACLE_URL"))
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("password", "temp123")
      .option("user", "EQ_JAVA_USER")
      .option("dbtable", "T_XA_TITAN_UPSTREAM_EVENT").load()*/



  //  eventData.printSchema()
    /* val df =  sqlContext.sql("SELECT * FROM T_XA_TITAN_UPSTREAM_MSG WHERE UPSTREAM_MSG_SID_I=370000161729")
     df.show()*/
    new C()
  }

  //Read write parquest file format
 /* val data = Seq(("James ","","Smith","36636","M",3000),
    ("Michael ","Rose","","40288","M",4000),
    ("Robert ","","Williams","42114","M",4000),
    ("Maria ","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  )
  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")*/
/*  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)*/
}

trait A {

  def ade(): Int = {
    return 1
  }
  def ape();
}


  abstract  class B (a: Int){
    def ade(): Int = {
      println("in class B"+a)
      return 1
    }
  }

    class C  extends B(2)  with A{
      override def ape(): Unit = {
        return 1
      }

    }
