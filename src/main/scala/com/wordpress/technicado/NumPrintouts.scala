package com.wordpress.technicado

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession}
import Constants._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

import scala.util.parsing.json.JSON



object NumPrintouts {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("USAGE: spark-submit --class com.wordpress.technicado.NumPrintouts --master local[*] " +
        "/path/to/application.jar hdfs:///path/to/configuration/file")
      System.exit(-1)
    }

    val spark: SparkSession = SparkSession.builder.getOrCreate
    ConfigReader.readConfig(args(0), spark.sparkContext)

    import spark.implicits._

    val df: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigReader.getString(NP_KAFKA_BROKERS))
      .option("subscribe", ConfigReader.getString(NP_KAFKA_TOPIC))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val eventsDS: Dataset[UserEvent] = df.mapPartitions(iter => {
      iter.map(str => {
        JSON.parseRaw(str).get.asInstanceOf[UserEvent]
      })
    })

    val printouts =  eventsDS
      .groupBy("action").count()

    val query = printouts
      .writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()

    query.awaitTermination()

  }

}
