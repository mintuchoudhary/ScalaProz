package com.m2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

import scala.util.Try

object XpathFileReader {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()
    val data = spark.read.option("multiline", "true").json("src\\main\\resources\\nested.json")
    data.show(false)

    val safeCol = safeColumn(data)
    val outData = data.select(
      safeCol("diuretic.name") as "name",
      safeCol("diuretic.nofield") as "field_present"
    )
      .setOptionalColToNull(List("a", "b", "c"))
    outData.show(false)


    val columnArray = EnricherConstats.COLUMN_LIST.map(value => safeCol(value._1).as(value._2)).toArray
    data.select(columnArray: _*).show(false)
  }

  def safeColumn(df: DataFrame) = (path: String) => {
    if (Try(df(path)).isSuccess == true) {
      df(path)
    }
    else {
      lit(null).cast(StringType)
    }
  }

  implicit class ImplicitClassInternal(dataframe: DataFrame) {
    val safeCol = safeColumn(dataframe)

    def setOptionalColToNull(listOfPath: List[String]): DataFrame = {
      var newDf: DataFrame = dataframe
      for (path <- listOfPath) {
        newDf = newDf.withColumn(path, safeCol(path))
      }
      newDf.show(false)
      newDf
    }
  }

}

object EnricherConstats {
  val MINERAL_NAME = "mineral.name"
  val MINERAL_DOSE = "mineral.dose"

  val COLUMN_LIST = Map(
    MINERAL_NAME -> "MINERAL_NAME",
    MINERAL_DOSE -> "MINERAL_DOSE"
  )
}