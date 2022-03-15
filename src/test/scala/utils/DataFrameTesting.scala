//package utils
//
//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.StructType
//import org.specs2.matcher.{Expectable, Matcher}
//import org.specs2.mutable.Specification
//import org.specs2.mutable.SpecificationWithJUnit
//
//object DataFrameTesting {
//
//  /**
//    * Approximate equality between 2 rows, based on equals from [[org.apache.spark.sql.Row]]
//    *
//    * @param r1  left row to compare
//    * @param r2  right row to compare
//    * @param tol max acceptable tolerance for comparing Double values, should be less than 1
//    * @return (true if equality respected given the tolerance, false if not; error message)
//    */
//  def rowApproxEquals(r1: Row, r2: Row, tol: Double): (Boolean, String) = {
//    if (r1.length != r2.length) {
//      return (false, "rows don't have the same length")
//    } else {
//      var idx = 0
//      val length = r1.length
//      while (idx < length) {
//        if (r1.isNullAt(idx) != r2.isNullAt(idx))
//          return (false, s"there is a null value on column ${r1.schema.fieldNames(idx)}")
//
//        if (!r1.isNullAt(idx)) {
//          val o1 = r1.get(idx)
//          val o2 = r2.get(idx)
//          o1 match {
//            case b1: Array[Byte] =>
//              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) return (false, s"$b1 is not equal to ${o2.asInstanceOf[Array[Byte]]} on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//
//            case f1: Float =>
//              if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(o2.asInstanceOf[Float])) return (false, s"null value on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//              if (Math.abs(f1 - o2.asInstanceOf[Float]) > tol) return (false, s"$f1 is not equal to ${o2.asInstanceOf[Float]} on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//
//            case d1: Double =>
//              if (java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(o2.asInstanceOf[Double])) return (false, s"null value on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//              if (Math.abs(d1 - o2.asInstanceOf[Double]) > tol) return (false, s"$d1 is not equal to ${o2.asInstanceOf[Double]} at $tol tolerance on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//
//            case d1: java.math.BigDecimal =>
//              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) return (false, s"$d1 is not equal to ${o2.asInstanceOf[java.math.BigDecimal]} on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//
//            case _ =>
//              if (o1 != o2) return (false, s"$o1 is not equal to $o2 on column ${r1.schema.fieldNames(idx)} (column index $idx)")
//          }
//        }
//        idx += 1
//      }
//    }
//    (true, "ok")
//  }
//
//}
//
//
///**
// * Utility class to compare DataFrames and Rows inside unit tests
// */
//trait DataFrameTesting extends Specification with SpecificationWithJUnit {
//
//  val maxUnequalRowsToShow = 10
//
//  /**
//   * Utility method to create dataframes from a sequence
//   *
//   * @param sqlContext
//   * @param seq    content of dataframe
//   * @param schema schema of dataframe
//   * @return datafram with schema and content
//   */
//  def createTestDataframe(sqlContext: SQLContext, seq: Seq[Row], schema: StructType): DataFrame = {
//    sqlContext.createDataFrame(
//      sqlContext.sparkContext.parallelize(seq), schema
//    )
//  }
//
//  /**
//   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that matches
//   * checks if the rows are equal.
//   */
//  def dataFrameEquals(res: DataFrame, tol: Double = 0.01): Matcher[DataFrame] = new Matcher[DataFrame] {
//    def apply[S <: DataFrame](s: Expectable[S]) = {
//
//      val expected: DataFrame = s.value
//
//      if (expected.schema != res.schema) {
//        result(test = false,
//          "same schema",
//          s"Schema of left dataframe \n ${expected.schema} \n " +
//            s"is different from schema of right dataframe \n ${res.schema} \n",
//          s)
//      } else if (expected.rdd.count != res.rdd.count) {
//        result(test = false,
//          "same length",
//          s"Length of left dataframe ${expected.rdd.count} is different from length of right dataframe ${res.rdd.count}",
//          s)
//      }
//      else {
//        try {
//          val toleranceValue = tol // this is to prevent serialization failures later on
//
//          expected.rdd.cache
//          res.rdd.cache
//
//          val expectedIndexValue = zipWithIndex(expected.rdd)
//          val resultIndexValue = zipWithIndex(res.rdd)
//
//          val unequalContent = expectedIndexValue.join(resultIndexValue)
//            .map { case (idx, (r1, r2)) => (idx, (r1, r2), DataFrameTesting.rowApproxEquals(r1, r2, toleranceValue))
//            }
//            .filter { case (idx, (r1, r2), (isEqual, errorMessage)) => !isEqual }
//            .map { case (idx, (r1, r2), (isEqual, errorMessage)) => s"On row index $idx, \n left row ${r1.toString()} \n right row ${r2.toString()} \n $errorMessage" }
//            .collect()
//
//          result(unequalContent.isEmpty,
//            "same dataframe",
//            s"${unequalContent.mkString("\n")}",
//            s)
//
//        } finally {
//          expected.rdd.unpersist()
//          res.rdd.unpersist()
//        }
//      }
//    }
//  }
//
//  private def zipWithIndex[U](rdd: RDD[U]) = rdd.zipWithIndex().map { case (row, idx) => (idx, row) }
//
//}