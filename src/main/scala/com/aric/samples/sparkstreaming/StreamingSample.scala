package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions.{explode, split}

object StreamingSample extends App {
  val spark = SparkSession.builder().appName("WordCounter").master("local[*]").getOrCreate();
  val lines = spark.readStream.textFile("D:\\Dev\\workspaces\\scala\\sparkstreaming\\src\\main\\resources\\input\\*.txt")
  import spark.implicits._
  
  val words = lines.select(explode(split($"value", " ")).as("word"))
  
  val wordCounts = words.groupBy("word").count()
  
  val q = wordCounts.
    writeStream.
    outputMode(OutputMode.Complete()).
    format("console").
    option("nRows", 30).
    start()
  q.awaitTermination()
}