import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Properties

object Factory {
  def createContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(Properties.envOrElse("DIGEST_APP_NAME", "DigestData"))
      .set("spark.cassandra.connection.host", Properties.envOrElse("DIGEST_CASSANDRA_HOST", "cassandra:7000"))

    new StreamingContext(conf, Seconds(1))
  }

  def createStream(streamingContext: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Properties.envOrElse("DIGEST_KAFKA_SERVERS", "kafka:9092"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Properties.envOrElse("DIGEST_CONSUMER_GROUP_ID", "ConsumerGroup"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val topicArray = sys.env("DIGEST_TOPICS").split(",")

    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicArray, kafkaParams)
    )
  }

  def start(streamingContext: StreamingContext): Unit = {
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
