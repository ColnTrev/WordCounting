package main.scala

import org.apache.spark.{SparkConf, SparkContext}
object Main {

  def main(args: Array[String]) {
    val inPath = args(0)
    val outPath = args(1)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inPath);
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    counts.foreach(println)
    counts.saveAsTextFile(outPath);
  }
}
