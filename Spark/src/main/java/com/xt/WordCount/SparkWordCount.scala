package com.xt.WordCount

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("Spark Count"))
    val threshold = 2

    // split each document into words
    val tokenized = sc.textFile("F:\\工作\\转正申请.txt").flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1))
      .reduceByKey(_ + _)

    System.out.println(charCounts.collect().mkString(", "))
  }
}
