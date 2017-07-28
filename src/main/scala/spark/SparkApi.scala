package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by killer on 2017/7/28.
  */
object SparkApi {
  val conf = new SparkConf().setAppName("sparkAPi").setMaster("local")
  val sc = new SparkContext(conf)
  val ss = new SparkSession()

  def main(args: Array[String]): Unit = {
    map()
    flatMap()
    filter()
    distinct()
    sample()
  }

  /**
    * map执行数据集的转换操作，将RDD中每个元素执行f函数生成新的数据
    */
  def map(): Unit = {
    val arr = Array((1, "a"), (2, "b"), (3, "c"))
    val rdd = sc.parallelize(arr).map(f => ("A" + f._1 * 10, f._2 + "#"))
    println(rdd.count())
    println(rdd.collect().mkString(","))
  }

  /**
    * 作用同map，不过一个元素可能生成多个结果数据。
    */
  def flatMap(): Unit = {
    val arr = Array("1#2#4", "a#b")
    val rdd = sc.parallelize(arr).flatMap(f => f.split("#"))
    println(rdd.count())
    println(rdd.collect().mkString("_"))
  }

  /**
    * 过滤操作，返回函数f为true的数据
    */
  def filter(): Unit = {
    val arr = Array("1@3@5", "a@b")
    val rdd = sc.parallelize(arr).filter(f => f.length > 4)
    println(rdd.collect().mkString("_"))
  }

  /**
    * 去重操作。
    */
  def distinct(): Unit ={
    val arr = Array(1,1,2,3,3,4,5,6)
    val rdd = sc.parallelize(arr).distinct()
    println(rdd.collect().mkString(","))
  }

  /**
    * 随机取样本。
    * 第一个参数如果为true,可能会有重复的元素，如果为false，不会有重复的元素；
    * 第二个参数取值为[0,1]，最后的数据个数大约等于第二个参数乘总数；
    * 第三个参数为随机因子。
    */
  def sample (): Unit ={
    val arr = 1 to 20
    val rdd = sc.parallelize(arr,3)
    val a = rdd.sample(true,0.5,10)
    val b = rdd.sample(false,0.5,10)

    println("a:"+a.collect().mkString(","))
    println("b:"+b.collect().mkString(","))
  }
}
