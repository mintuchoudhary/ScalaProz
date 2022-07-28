package com.m2.utils

import java.io.Serializable

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.{Success, Try}

object CommonUDFUtils {

  val RATES_QUERY = "select * from %s.rates where date=%s"
  def getBackToBackId = udf((ids: Seq[String], sysType: Seq[String]) => {
    val validValues = Seq("valid1", "valid2")
    var value = ""
    for (requiredSystemType <- validValues) {
      if (null != sysType && sysType.contains(requiredSystemType)
        && StringUtils.isEmpty(value)) {
        val index = sysType.indexOf(requiredSystemType)
        if (ids != null && ids(index) != null) {
          value = ids(index)
        }
      }
    }
    // Checks if the value is being populated or not
    if (StringUtils.isNotBlank(value)) {
      Some(value)
    } else {
      None
    }
  })

  def getStructId = udf((tradid: Seq[String], stype: Seq[String]) => {

    if (stype != null && stype.contains("STRUCTURE")) {
      val index = stype.indexOf("STRUCTURE")
      if (tradid != null && tradid(index) != null) {
        tradid(index)
      } else {
        null
      }
    } else {
      null
    }
  })

  def getStreamId: UserDefinedFunction = udf((dealRp: String, dealidid: String,
                                              systype: mutable.WrappedArray[String],
                                              ssystem: mutable.WrappedArray[String],
                                              stid: mutable.WrappedArray[String]) => {
    var output = Array[UpIdCase]()

    if (systype != null && ssystem != null && ssystem != null) {
      (systype, ssystem, stid).zipped.map { (ust, us, uti) =>
        if (StringUtils.equalsAnyIgnoreCase(ust, "Platform", "Capture", "System"))
          output = output :+ UpIdCase(ust, us, uti)
      }
    }

    if (StringUtils.equalsIgnoreCase(dealRp, "IST")) {
      output = output :+ UpIdCase("CAPTURE", "IST", dealidid)
    }
    output
  })

  def getRates(sparkSession: SparkSession, envParametersMap: Map[String, String]): DataFrame = {
    val hivedb = envParametersMap("HIVEDB")
    val cobDate = envParametersMap("DATE")
    val ratesDF = sparkSession.sql(RATES_QUERY.format(hivedb, cobDate))
    ratesDF
  }
////
////  /**
////   * Fetches the underlying ID depending upon the source system.
////   *
////   * @return String
//   */
  def getUdDtls = functions.udf((uIds: Seq[Row], udIdsTp: Seq[Row],
                                 uIdType: String, ssid: String) => {
    var output = Array[UDetailsCase]()

    if (StringUtils.equalsIgnoreCase(ssid, "scap")) {
      if (null != uIds && uIds.nonEmpty) {
        for (uInst <- uIds) {
          if (null != uInst) {
            val instrId = uInst.getAs("uidId").asInstanceOf[GenericRowWithSchema]
            if (null != instrId) {
              val underlyingid = instrId.getAs("id").asInstanceOf[String]
              if (StringUtils.isNotEmpty(underlyingid))
                output = output :+ UDetailsCase(underlyingid, uIdType)
            }
          }
        }
      }
    }
    else if (StringUtils.equalsIgnoreCase(ssid, "TP_CAP")) {
      if (null != udIdsTp) {
        for (uIdTp <- udIdsTp) {
          if (null != uIdTp) {
            Try(uIdTp.getAs("refObl").asInstanceOf[GenericRowWithSchema]) match {
              case Success(refObl) =>
                if (null != refObl) {
                  val uid = refObl.getAs("id").asInstanceOf[String]
                  val uidtype = refObl.getAs("domain").asInstanceOf[String]
                  if (StringUtils.isNotEmpty(uid) || StringUtils.isNotEmpty(uidtype))
                    output = output :+ UDetailsCase(uid, uidtype)
                }
              case _ => output
            }
          }
        }
      }
    }
    output
  })

  /**
   * Converts the DataFrames having String type to Array Type.
   *
   * @param df
   * @param columnNames
   * @return
   */
  def convertStringTypeToArrayType(df: DataFrame, columnNames: Seq[String]): DataFrame = {
    var normalizedDf = df
    for (columnName <- columnNames) {
      normalizedDf = normalizeArrayType(normalizedDf, columnName)
    }
    normalizedDf
  }

  /**
   * Converts the string type to array type.
   *
   * @param df
   * @param columnName
   * @return
   */
  def normalizeArrayType(df: DataFrame, columnName: String): DataFrame = {
    df.schema(columnName).dataType match {
      case StringType =>
        val modDf = df.withColumn(columnName, array(col(columnName)))
        modDf
      case _ => df
    }
  }
  /**
   * This UDF gets the region based on the upstreamsystem.
   * By default, the region is set to NEW_YORK
   *
   * @param ssRegionMap
   * @return
   */
  def getRates(ssRegionMap: scala.collection.Map[String, String]): UserDefinedFunction = udf((uIdArray: mutable.WrappedArray[Row]) => {
    var value: String = "NEW_YORK"
    val loop = new Breaks
    loop.breakable {
      if (null != uIdArray) {
        uIdArray.foreach(element => {
          if (StringUtils.equalsIgnoreCase(element.getAs("stype"), "ISYSTEM")) {
            value = ssRegionMap.getOrElse(element.getAs("ssystem"), "NY")
            loop.break
          }
        })
      }
    }
    value
  })

case class UDetailsCase(id: String, types: String) extends Serializable
case class UpIdCase(id:String, value2:String,s3:String) extends Serializable
}
