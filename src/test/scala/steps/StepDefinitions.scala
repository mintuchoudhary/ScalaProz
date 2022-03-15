package scala.steps

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.cucumber.datatable.DataTable
import org.apache.spark.sql.types.IntegerType
import utils.DataFrameHelper
//import cucumber.api.DataTable
import io.cucumber.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class StepDefinitions extends ScalaDsl with EN with DatasetComparer {

  val spark = SparkSession.builder().appName("Test Cucumber").master("local")
    .config("spark.sql.warehouse.dir", "c:/tmp/")
    .config("spark.testing.memory", "2147480000")
    //  .enableHiveSupport()
    .getOrCreate()

  val dataFrame: DataFrame = null
  Given("""a chicken collects {int} insects per minute""") { (int1: Int) =>
    // Write code here that turns the phrase above into concrete actions
    println("inside given block " + int1)

  }

  When("""chicken has searched insects for {int} minutes""") { (int1: Int) =>
    // Write code here that turns the phrase above into concrete actions
    println("inside when block " + int1)
    //throw new io.cucumber.scala.PendingException()
  }

  Then("""the chicken has found {int} insects""") { (int1: Int) =>

    println("inside then block " + int1)
    //throw new io.cucumber.scala.PendingException()
  }

  When("""^chicken has searched insects for (\d+) hours$""") { (int1: Int) =>

    println("inside when block " + int1)
    //throw new io.cucumber.scala.PendingException()
  }
  Given("""I have the following books in the store""") { (datatable: DataTable) =>

    val dataframe = DataFrameHelper.convertDataTableToDataFrame(datatable, spark)
    //dataframe.show(false)

  }

  When("""Available book in store""") { (bookDataTable: DataTable) =>

    val bookDF = DataFrameHelper.convertDataTableToDataFrame(bookDataTable, spark)
    //bookDF.show(false)
    val exceptedDF = bookDF.withColumn("availability", lit(15).cast(IntegerType))
    exceptedDF.createOrReplaceTempView("outputTable")
    val outputTable = spark.table("outputTable").filter(col("id")===1)
    println("before output table::")
    outputTable.show(false)
    assertSmallDatasetEquality(bookDF, exceptedDF)
  }

}