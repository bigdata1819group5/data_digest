import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object Main {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, topics, groupID) = args

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupID,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf().setAppName("DigestData")
    val streamingContext = new StreamingContext(conf, Seconds(1))
    val topicArr = topics.split(",")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicArr, kafkaParams)
    )

    stream.map(record => (record.key, record.value))


    streamingContext.start()             // Start the computation
    streamingContext.awaitTermination()  // Wait for the computation to terminate
  }

}
