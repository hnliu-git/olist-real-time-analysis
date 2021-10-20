package sparkstructuredstreaming

import java.util.HashMap
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

object KafkaSpark {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ConsumerSent")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sentiment")
      .load()
    
    import spark.implicits._

    // get the value col
    val pairs = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      
    import org.apache.spark.sql._
    import org.apache.spark.sql.streaming._
      
    def updateStatus(
        k: String,
        actions: Iterator[Row],
        state: GroupState[Int]): (String, Int) = {
            val status = state.getOption.getOrElse(0)
            var status_var = status
            actions.foreach { action =>
                if (action.getString(1) == "pos"){
                    status_var = status_var + 1
                }
                
            }
            
            state.update(status_var)
            (k, status_var)
    }
    
      
    val res = pairs
      .groupByKey(x => (x.getString(0)))
      .mapGroupsWithState(updateStatus _)
      .toDF("key", "pos_cot")
    
      
    val query = res.writeStream
      .format("console")
      .outputMode("update")
      .start()

    query.awaitTermination()
    
  }
}
