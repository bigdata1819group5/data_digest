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
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupID,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf()
      .setAppName("DigestData")
      .set("spark.cassandra.connection.host", cassandraHost)

    val streamingContext = new StreamingContext(conf, Seconds(1))
    val topicArr = topics.split(",")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicArr, kafkaParams)
    )

    val values = stream.map(record => record.value())
//    stream = stream.map(record => (record.key, record.value))
//    stream.map(line => { val arr = line.key.split(","); (arr(0).toInt, arr(1)) }).saveToCassandra("sparkdata", "sparktable", SomeColumns("sno", "pname"))
    values.foreachRDD(rdd => {
      val crdd = rdd.map(value => Tuple1(value))
      crdd.saveToCassandra("sparkData", "vehicleLocation", SomeColumns("location"))
    })

    streamingContext.start()             // Start the computation
    streamingContext.awaitTermination()  // Wait for the computation to terminate
  }

}
