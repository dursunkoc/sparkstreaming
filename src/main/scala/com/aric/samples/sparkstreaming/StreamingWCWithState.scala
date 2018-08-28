package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

object StreamingWCWithState extends App {
  val spark = SparkSession.builder().appName("StreamingWCWithState").master("local[*]").getOrCreate()

  val ssc = new StreamingContext(sparkContext = spark.sparkContext, batchDuration = Durations.seconds(10))
  
  ssc.checkpoint("d:\\dev\\tmp\\spark")

  val lines = ssc.socketTextStream(hostname = "localhost", port = 9999, storageLevel = StorageLevel.OFF_HEAP)

  val counts = lines
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKeyAndWindow(reduceFunc = _ + _, 
                          invReduceFunc = _ - _, 
                          windowDuration = Durations.seconds(30), 
                          slideDuration = Durations.seconds(10))
  counts.print()
  
  ssc.start()
  ssc.awaitTermination()
}