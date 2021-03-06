package sparkstructuredstreaming

import java.util.HashMap
import java.io._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.commons.io.FileUtils

object KafkaSpark {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ConsumerOrder")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "order")
      .load()
    
    import spark.implicits._

    // get the value col
    val pairs = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val products = pairs.map { row =>
        val group = row._2.split("\\[SEP\\]")
        group(1)
    }.toDF("product_id")
      
      
    import org.apache.spark.sql._
    import org.apache.spark.sql.streaming._
      
    def updateStatus(
        k: String,
        actions: Iterator[Row],
        state: GroupState[Int]): (String, Int) = {
            val status = state.getOption.getOrElse(0)
            var status_var = status
            actions.foreach { action =>
                status_var = status_var + 1
            }
            
            state.update(status_var)
            (k, status_var)
    }
    
      
    val res = products
      .groupByKey(x => (x.getString(0)))
      .mapGroupsWithState(updateStatus _)
      .toDF("product_id", "cot")
    
    val writerForText = new ForeachWriter[Row] {
        var fileWriter: FileWriter = _
        
        override def process(value: Row): Unit = {
            fileWriter.append(value.toSeq.mkString(",")+"\n")
        }
        
        override def close(errorOrNull: Throwable): Unit = {
            fileWriter.close()
        }
        
        override def open(partitionId: Long, version: Long): Boolean = {
            fileWriter = new FileWriter(new File(s"/home/osboxes/Projects/dic/project/data/top_purchased.csv"), true)
            true
        }
    }

    val query = res.writeStream
      .foreach(writerForText)
      .outputMode("update")
      .start()

    query.awaitTermination()
    
  }
}
