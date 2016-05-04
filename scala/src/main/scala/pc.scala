/**
  * Created by asus on 2016/4/25.
  */

import org.apache.spark.{SparkConf, SparkContext}

object pc {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Asus_PaymentCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("asus/bd_hw3/*.csv")
    val words = lines.flatMap(_.split(",")(11))
    val words_pairs = words.map((_, 1))
    val words_redc = words_pairs.reduceByKey(_+_)
	words_redc.collect().foreach(println)
  }
}