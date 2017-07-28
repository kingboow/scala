import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by killer on 2017/7/28.
  */
object WorkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("workCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    /*val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "cars.csv", "header" -> "true"))
    df.foreach(s => print("abc" + s.get(0)))
    df.show()*/
    val textfile = sc.textFile("file:///D:/project/scala/work.txt")
    textfile.foreach(s => println(s))
    val words = textfile.flatMap(s => s.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))

  }

}
