package com.m2.basic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ImplicitClass {

  implicit class ImplicitClassInternal(dataframe: DataFrame) {
    def invokeJoinOnDataFrame(joinFrame: DataFrame): DataFrame = {
      dataframe.join(joinFrame, dataframe("name") === joinFrame("name"), "inner")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = com.m2.utils.getSparkSession()
    import spark.implicits._
    val goalsDF = Seq(
      ("meg", 2), ("meg", 4), ("min", 3), ("min2", 1),
      ("ss", 1)).toDF("name", "goals")
    val nameDF = Seq("meg").toDF("name")
    goalsDF.show(false)
    goalsDF
      .withColumn("goals", when(col("goals") === 1, "1 goal").otherwise(concat_ws(" ", col("goals"), lit("goals")))
      )
      .withColumn("ID", monotonically_increasing_id())
      .invokeJoinOnDataFrame(nameDF).show(false)

    spark.close()
  }
}
