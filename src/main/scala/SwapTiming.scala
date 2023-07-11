
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.types._


import org.apache.spark.sql.functions._


object SwapTiming {

  def main(args: Array[String]): Unit = {

     val spark = SparkSession.builder()
       .appName("Datafame Demo")
       .config("spark.testing.memory", "2147480000")
       .config("spark.driver.memory", "2147480000")
       .master(master = "local[1]")
       .getOrCreate()

    val mytimDF = spark.read.option("header",true)
      .csv("E:/IdeaProjects/ScalaProz/src/main/resources/swap_in_out.csv")

    val win = Window.partitionBy("emp_code").orderBy("emp_code","swap_time")
    //val lagCol = lag(col("Salary"), 1).over(win)


   val nwDF =  mytimDF.withColumn("SwapNext",lag(col("swap_time"), 1).over(win))
     .filter("swap_type='OUT'")
     //show()

   val newhrDF = nwDF.withColumn("SwapINHours",to_timestamp(col("SwapNext"),"yyyy-MM-dd HH:mm:ss"))
    .withColumn("SwapOUTHours",to_timestamp(col("swap_time"),"yyyy-MM-dd HH:mm:ss"))
     .withColumn("SwapTotHours",(
       col("SwapOUTHours").cast(LongType)-col("SwapINHours").cast(LongType))/3600 )


//    val grpDF = newhrDF
//      .groupBy("emp_code")
//      .agg(sum("SwapTotHours"))
//      .show()

      val grppDF = newhrDF.createOrReplaceTempView("tblDiffHours")

      spark.sql("select emp_code, cast(sum(SwapTotHours) as decimal(10,2)) as total_hours_worked  from tblDiffHours group by emp_code ").show()


 }
  }
