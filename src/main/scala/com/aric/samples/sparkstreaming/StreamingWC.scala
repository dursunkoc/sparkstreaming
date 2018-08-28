package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

object StreamingWC extends App {
  val spark = SparkSession.builder().appName("StreamingWC").master("local[*]").getOrCreate()
  val scc = new StreamingContext(sparkContext = spark.sparkContext, batchDuration = Durations.seconds(2L))

  scc.checkpoint("d:\\dev\\tmp\\spark")
  val lines = scc.socketTextStream("localhost", 9999)

  val counts = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  counts.print()

  scc.start()
  scc.awaitTermination()
}