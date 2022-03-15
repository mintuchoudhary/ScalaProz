package utils

import io.cucumber.datatable.DataTable
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.FreeSpec

import scala.collection.JavaConverters._
import scala.collection.mutable

object DataFrameHelper {//extends FreeSpec with DataFrameComparer {

  def convertDataTableToDataFrame(datatable: DataTable, spark: SparkSession): DataFrame = {
    val headers = datatable.cells().get(0).asScala
    val headersAsString: mutable.Buffer[String] = headers // tops cells

    //println("listofString=" + headersAsString)
    val fieldSpec = headersAsString.map(_.split(":"))
      .map(split => (split(0), scala.util.Try(split(1)).getOrElse("String").toString.toLowerCase()
        , scala.util.Try(split(2)).getOrElse("true").toBoolean
      ))
      .map {
        case (name, "string", nullableFlag) => (name, StringType, nullableFlag)
        case (name, "double", nullableFlag) => (name, DoubleType, nullableFlag)
        case (name, "int", nullableFlag) => (name, IntegerType, nullableFlag)
        case (name, "integer", nullableFlag) => (name, IntegerType, nullableFlag)
        case (name, "boolean", nullableFlag) => (name, BooleanType, nullableFlag)
        case (name, "date", nullableFlag) => (name, DateType, nullableFlag)
        case (name, _, true) => (name, StringType, true)
      }

    //println("fieldSpec:::" + fieldSpec) //ArrayBuffer((id,StringType,true), (title,StringType,true),..)
    val schema = StructType(fieldSpec.map {
      case (name, datatypez, nullable) => StructField(name, datatypez, nullable)
    })
    //println("schema:::" + schema) //StructType(StructField(id,StringType,true),..)
    datatable.asMaps().asScala.map(row => println("values::" + row.values())) //ArrayBuffer(( 1, The Devil in the White City, Erik Larson))

    datatable.asMaps().asScala.map(row => row.values().asScala.zip(fieldSpec).map(data => println("data::" + data))) //List(1,(id,StringType,true))
    datatable.asMaps().asScala.map(row => println("mapped::" + row.values().asScala.zip(fieldSpec).map { case (value, (colName, dtype, nullable)) => (value, dtype, nullable) }))
    //List((1,StringType,true),..)


    val rows = datatable.asMaps().asScala
      .map { row =>
        val values = row.values.asScala.zip(fieldSpec).map {
          case (value, (_, dtype, nullable)) => (value, dtype, nullable) //(1,(id,StringType,true))
        }.map {
          case (v, DataTypes.IntegerType, nullable) => v.toInt  //(1,(id,Success,true))
          case (v, DataTypes.StringType, nullable) => v.toString
          case (v, _, nullable) => v.toString
        }.toSeq
        //println("final::"+ values)
        Row.fromSeq(values)
      }.toList
    println("final rows::"+ rows)

    val outDF= spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    println("final df::"+ outDF.printSchema())
    outDF
  }

//  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
//    if (!actualDF.schema.equals(expectedDF.schema)) {
//    //  throw new DataFrameSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
//    }
//    if (!actualDF.collect().sameElements(expectedDF.collect())) {
//      //throw new DataFrameContentMismatch(contentMismatchMessage(actualDF, expectedDF))
//    }
//  }
 // case class DataFrameSchemaMismatch(smth: String)   extends Exception(smth)
  //case class DataFrameContentMismatch(smth: String)   extends Exception(smth)

}

