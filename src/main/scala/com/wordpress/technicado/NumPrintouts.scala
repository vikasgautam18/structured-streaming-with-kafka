package com.wordpress.technicado

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import Constants._
import org.apache.spark.sql.streaming.OutputMode

object NumPrintouts {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("USAGE: spark-submit --class com.wordpress.technicado.NumPrintouts --master local[*] " +
        "/path/to/application.jar hdfs:///path/to/configuration/file")
      System.exit(-1)
    }

    val spark = SparkSession.builder.getOrCreate
    ConfigReader.readConfig(args(0), spark.sparkContext)

    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigReader.getString(NP_KAFKA_BROKERS))
      .option("subscribe", ConfigReader.getString(NP_KAFKA_TOPIC))
      .load()
    import spark.implicits._

    df.show()

    val eventsDS: Dataset[UserEvent] = df.as[UserEvent]

    val printouts = eventsDS.filter(_.action == "PRINT").groupBy("value").count()

    val query = printouts.writeStream.outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()

  }

}
