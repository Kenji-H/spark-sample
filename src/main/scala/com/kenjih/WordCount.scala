package com.kenjih

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()

    val inputFile = args(0)
    val outputFile = args(0)

    val text = sc.textFile(inputFile)
    val res = run(text)

    res.saveAsTextFile(outputFile)
  }

  def run(text: RDD[String]): RDD[(String, Int)] = {
    text.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
  }
}