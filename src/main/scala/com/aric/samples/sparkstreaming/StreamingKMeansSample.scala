package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.clustering.StreamingKMeans
import java.util.concurrent.TimeUnit
import org.apache.spark.streaming.scheduler.StreamingListener

object StreamingKMeansSample extends App {

  val spark = SparkSession.builder().appName("StreamingKMeans").master("local[*]").getOrCreate()
  val scc = new StreamingContext(spark.sparkContext, Durations.seconds(10))
//  scc.checkpoint("d:\\dev\\tmp\\spark")

  val trainingStream = //scc.textFileStream("file:///D://Dev//workspaces//scala//sparkstreaming//src//main//resources//ml//training")
  scc.socketTextStream("localhost", 9999)
    .map(_.split(","))
    .map(cells => Vectors.dense(cells(0).toDouble, cells(1).toDouble))
//decayfactor = 0 Cluster centers: [23.30131272438292,-78.62682241335514],[8.414173507507451,104.54618794054963]
//decayfactor = 0 Cluster centers: [25.85968667168262,-78.02963003009576],[11.44938229739777,103.8897683605948]
  val model = new StreamingKMeans(k=2, decayFactor=1.0, timeUnit="batches").setRandomCenters(2, 1.0, 0)
  println(s"Initial centers: ${model.latestModel().clusterCenters.addString(new StringBuilder(), ",")}")

  model.trainOn(trainingStream)

  new Thread() {
    override def run() {
      while (true) {
        Thread.sleep(10000L);
        println(s"Cluster centers: ${model.latestModel().clusterCenters.addString(new StringBuilder(), ",")}")
      }
    }
  }.start();
    
  scc.start()
  scc.awaitTermination()
}