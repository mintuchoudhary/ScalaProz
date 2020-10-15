import org.apache.spark.sql.SparkSession

object AggregationDemo {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("AccumulatorDemo").master("local").getOrCreate();
    sc.sparkContext.setLogLevel("ERROR") //basic INFO still prints

    import sc.implicits._

    val goalsDF = Seq(
      ("meg", 2),
      ("meg", 4),
      ("mintu", 3),
      ("mintu", 1),
      ("ss", 1)
    ).toDF("name", "goals")

    // println(goalsDF.show(false))

    import org.apache.spark.sql.functions._

    goalsDF
      .groupBy("name")
      .agg(sum("goals")) //without groupBy it is just sum all data
      .show()
    //OR
    goalsDF
      .groupBy("name") //return RelationalGroupedDataset
      .sum() //without groupby you cannot directly call sum
      .show()

    val studentsDF = Seq(
      ("mario", "italy", "europe"),
      ("stefano", "italy", "europe"),
      ("victor", "spain", "europe"),
      ("mintu", "india", "asia"),
      ("meg", "india", "asia"),
      ("vito", "italy", "europe")
    ).toDF("name", "country", "continent")

    studentsDF
      .groupBy("continent", "country")
      .count()
      .show()

    val hockeyPlayersDF = Seq(
      ("mintu", 40, 102, 1990),
      ("mintu", 41, 122, 1991),
      ("mintu", 31, 90, 1992),
      ("meg", 33, 61, 1989),
      ("meg", 45, 84, 1991),
      ("meg", 35, 72, 1992),
      ("meg", 25, 66, 1993)
    ).toDF("name", "goals", "assists", "season")

    hockeyPlayersDF
      .groupBy("name")
      .agg(avg("goals").as("goals_avg"), round(avg("assists"), 2))
      .where(col("goals_avg") >= 30)
      .show()

    //The cube function “takes a list of columns and applies aggregate expressions to all possible combinations of the grouping columns”.

    val dataDF = Seq(
      ("mintu", 2L),
      ("mintu", 2L),
      ("meg", 1L),
      ("meg", 2L)
    ).toDF("word", "num")

    dataDF.cube("word", "num")
      .count()
      .show()

  }
}

/**
 * +-----+----------+
 * | name|sum(goals)|
 * +-----+----------+
 * |  meg|         6|
 * |   ss|         1|
 * |mintu|         4|
 * +-----+----------+
 *
 *
 * +---------+-------+-----+
 * |continent|country|count|
 * +---------+-------+-----+
 * |   europe|  italy|    3|
 * |   europe|  spain|    1|
 * |     asia|  india|    2|
 * +---------+-------+-----+
 *
 * +-----+----+-----+
 * | word| num|count|
 * +-----+----+-----+
 * |mintu|   2|    2| //count 2 for these entries
 * |  meg|   1|    1| //count 1 for these entries
 * | null|null|    4| //count 4 for these entries
 * |mintu|null|    2| //word has 2  entry
 * | null|   1|    1|//num has 1  entry
 * | null|   2|    3|//num has 3  entry
 * |  meg|   2|    1|//count 1 for these entries
 * |  meg|null|    2|//word has 2  entry
 * +-----+----+-----+
 *
 */
