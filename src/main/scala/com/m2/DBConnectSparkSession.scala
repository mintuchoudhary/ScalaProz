/*
package com.m2


import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object DBConnectSparkSession {


    def main(args: Array[String]) {

      println("Trying to connect to database... ")
      val sparksession = SparkSession.builder().appName("ConnectingToOracleDatabase").master("local").getOrCreate();

//      Config config = ConfigFactory.load();
      val eventData = sparksession.read.format("jdbc")
        .option("url", ConfigFactory.load().getString("ORACLE_URL"))
        .option("driver",ConfigFactory.load().getString("DRIVER"))
        .option("password", ConfigFactory.load().getString("PASSWORD"))
        .option("user", ConfigFactory.load().getString("USER_NAME")).load()
        //.option("dbtable", "T_XA_TITAN_UPSTREAM_EVENT").load()

      eventData.sqlContext.sql("SELECT * FROM T_XA_TITAN_UPSTREAM_EVENT")
      eventData.printSchema()
      eventData.show()
      /* val df =  sqlContext.sql("SELECT * FROM T_XA_TITAN_UPSTREAM_MSG WHERE UPSTREAM_MSG_SID_I=370000161729")
       df.show()*/

    }


}
*/
