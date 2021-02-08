package com.m2

import org.apache.spark.sql.SparkSession
// Convert row data to columns...
// note - after pivot method.. some df methods to be called (count, agg, min ,max,sum)
object PivotDataExample {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pivot data").master("local").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()
  }
}


/**Output:
 +-------+------+-------+
|Product|Amount|Country|
+-------+------+-------+
| Banana|  1000|    USA|
|Carrots|  1500|    USA|
|  Beans|  1600|    USA|
| Orange|  2000|    USA|
| Orange|  2000|    USA|
| Banana|   400|  China|
|Carrots|  1200|  China|
|  Beans|  1500|  China|
| Orange|  4000|  China|
| Banana|  2000| Canada|
|Carrots|  2000| Canada|
|  Beans|  2000| Mexico|
+-------+------+-------+

+-------+------+-----+------+----+
|Product|Canada|China|Mexico| USA|
+-------+------+-----+------+----+
| Orange|  null| 4000|  null|4000|
|  Beans|  null| 1500|  2000|1600|
| Banana|  2000|  400|  null|1000|
|Carrots|  2000| 1200|  null|1500|
+-------+------+-----+------+----+
 Here - USA has 2 records for Orange, after sum (2000+2000) the record becomes as 1
 */
