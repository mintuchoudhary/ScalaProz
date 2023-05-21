import com.m2.utils
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object FindFirstNotNull {
  def main(args: Array[String]): Unit = {
    val spark = utils.getSparkSession()
  }

  def getDType(df: DataFrame, multiCol: ArrayBuffer[Column]): DataType = {
    var dataType: Array[DataType] = Array.empty
    val nonNullCol = df.select(multiCol: _*).columns.map(c => when(col(c).isNotNull, lit(c)))
    df.select(coalesce(nonNullCol: _*)).alias("newCol").show(false)
    val notNullColList = df.select(coalesce(nonNullCol: _*)).alias("newCol").na.drop().collectAsList()

    if(notNullColList.size() > 0) {
      val notNullCol = notNullColList.get(0).getAs("newCol").toString

      dataType = df.select(notNullCol).schema.fields.map { field =>
        field.dataType

      }
      println(notNullCol +"::"+ dataType(0))
    }
      if(dataType.size > 0)
        dataType(0)
      else
        StringType
  }
}
