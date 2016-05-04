/**
  * Created by asus on 2016/4/25.
  */

import org.apache.spark.{SparkConf, SparkContext}

object wc {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Asus_wordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("asus/bd_hw3/README.md")
    val words = lines.flatMap(_.split(" "))
    val words_pairs = words.map((_, 1))
    val words_redc = words_pairs.reduceByKey(_+_)
	words_redc.collect().foreach(println)
  }
}