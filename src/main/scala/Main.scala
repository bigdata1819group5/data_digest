import scala.util.Properties

import com.datastax.spark.connector._

object Main {
  def main(args: Array[String]): Unit = {

    val streamingContext = Factory.createContext()
    val stream = Factory.createStream(streamingContext)
      .map(record => record.value)


    val serialized = stream.map(value => Vehicle.create(value))
    val tiled = serialized.map(v => Vehicle.makeTiled(v))

    val keyspace = Properties.envOrElse("DIGEST_CASSANDRA_KEYSPACE", "streaming")
    val table = Properties.envOrElse("DIGEST_CASSANDRA_TABLE", "vehicles_by_tileid")

    tiled.foreachRDD(rdd => {
      rdd.saveToCassandra(keyspace, table)
    })

    Factory.start(streamingContext)
  }
}
