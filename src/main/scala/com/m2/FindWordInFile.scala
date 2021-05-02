package com.m2;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FindWordInFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("AccumulatorDemo").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //Define a schema for file
    val schema = StructType(Array(StructField("EmpId", IntegerType, false),
      StructField("EmpName", StringType, false),
      StructField("Salary", DoubleType, false),
      StructField("SalaryDate", DateType, false)
    ))
    val fileRDD = spark.read.format("com.databricks.spark.csv") //when format not given it looks for parquet file
      .option("header", "false")
      .option("dateFormat", "MM/dd/yyyy") //if date format not given all data will be null
      .schema(schema)
      .load("src/main/resources/employee.dat")
    fileRDD.show()


    val makeNullCols: Seq[String] = Seq("EmpName")
    val dataWithNullCol = makeNullCols.foldLeft(fileRDD)((df, columnName) =>
      df.withColumn(columnName, lit(null))
    )
    dataWithNullCol.show()

    //Fill null values with DEFAULT NA
    //  val finalDf = dataWithNullCol.na.fill("NA", makeNullCols)

    println(List(1,3,5).fold(0)(_ + _))
    println(List(1,3,5).fold(0)((sum,x) => sum + x))
    val lagWindow = lag(col("Salary"), 1).over(Window.orderBy("salarydate"))
    val leadCol = lead(col("Salary"), 1).over(Window.orderBy("salarydate"))


    dataWithNullCol.withColumn("lag", lagWindow)
      .withColumn("LeadCol", leadCol).show()


  }
}

/**
 * == Parsed Logical Plan ==
 * Relation[EmpId#0,EmpName#1,Salary#2,SalaryDate#3] csv
 * *
 * == Analyzed Logical Plan ==
 * EmpId: int, EmpName: string, Salary: double, SalaryDate: date
 * Relation[EmpId#0,EmpName#1,Salary#2,SalaryDate#3] csv
 * *
 * == Optimized Logical Plan ==
 * Relation[EmpId#0,EmpName#1,Salary#2,SalaryDate#3] csv
 * *
 * == Physical Plan ==
 * (1) FileScan csv [EmpId#0,EmpName#1,Salary#2,SalaryDate#3] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/choumin/learnz/ScalaMavenProz/src/main/resources/employee.dat], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<EmpId:int,EmpName:string,Salary:double,SalaryDate:date>
 *
 */
