import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.datastax.spark.connector._

object Main {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, topics, groupID, cassandraHost) = args

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ModelDeserializer],
      "group.id" -> groupID,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf()
      .setAppName("DigestData")
      .set("spark.cassandra.connection.host", cassandraHost)

    val streamingContext = new StreamingContext(conf, Seconds(1))
    val topicArr = topics.split(",")

    val stream = KafkaUtils.createDirectStream[String, Model](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Model](topicArr, kafkaParams)
    )

    val values = stream.map(record => record.value)
    values.foreachRDD(rdd => {
      rdd.saveToCassandra("sparkdata", "detaillocation")
    })

    streamingContext.start()             // Start the computation
    streamingContext.awaitTermination()  // Wait for the computation to terminate
  }
}
