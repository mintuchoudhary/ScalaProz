import com.m2.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Add a column to RDD,
 * Convert RDD to DataFrame, and add new column
 */
object RDDAddColumn {
  def main(args: Array[String]): Unit = {

    val spark = utils.getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    val fileRDD = spark.sparkContext.textFile("src/main/resources/data")
    fileRDD.foreach(println)

    val newRDD = fileRDD.map(lines => {
      val fields = lines.split(",")
      if (fields(1).toInt > 18)
        (fields(0), fields(1), fields(2), "Y")
      else
        (fields(0), fields(1), fields(2), "N")
    })
    newRDD.foreach(println)

    /** ***********Convert RDD to DataFrame ****************/
    import spark.implicits._
    val dataDF = fileRDD.
      map(line => {
        val fields = line.split(",")
        (fields(0), fields(1), fields(2))
      }).
      toDF("name", "age", "city")
    dataDF.show(false)

    dataDF.withColumn("eligible", when(col("age") > 18, "Y").otherwise("N")).show()
  }
}

/**
 * Mintu,27,Pune
 * Mg,25,PUN
 * SS,26,Mum
 * DD,17,Chennai
 * (Mintu,27,Pune,Y)
 * (Mg,25,PUN,Y)
 * (SS,26,Mum,Y)
 * (DD,17,Chennai,N)
 * +-----+---+-------+
 * |name |age|city   |
 * +-----+---+-------+
 * |Mintu|27 |Pune   |
 * |Mg  |25 |PUN    |
 * |SS   |26 |Mum    |
 * |DD   |17 |Chennai|
 * +-----+---+-------+
 * *
 * +-----+---+-------+--------+
 * | name|age|   city|eligible|
 * +-----+---+-------+--------+
 * |Mintu| 27|   Pune|       Y|
 * |  Mg| 25|    PUN|       Y|
 * |   SS| 26|    Mum|       Y|
 * |   DD| 17|Chennai|       N|
 * +-----+---+-------+--------+
 */