/*
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  /* This is my first java program.
  * This will print 'Hello World' as the output
  */
  def main(args: Array[String]) {
    val conf = new SparkConf();
    println("Hello, world!") // prints Hello World
    println("conf obj:"+conf)
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
  //  println("conf obj:"+sc)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
//val textFile = sc.textFile("src/main/resources/data.txt")
    val textFile = sc.textFile("hdfs://fraitdbox02607.de.m2.com:8020/user/devuser/input/data.txt") //to read HDFS file - will read part file
    textFile.foreach(println)

    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)


    counts.foreach(println)
    System.out.println("Total words:"+ counts.count())
    //counts.saveAsTextFile("src/main/resources/WordCountOfData3")
    counts.saveAsTextFile("hdfs://fraitdbox02607.de.m2.com:19000/tmp/WordCountOfData2")  // to load file to HDFS
  }
}*/
