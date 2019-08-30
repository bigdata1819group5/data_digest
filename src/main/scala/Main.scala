import scala.util.Properties

import com.datastax.spark.connector._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

object Main {
  def main(args: Array[String]): Unit = {

    val streamingContext = Factory.createContext()
    val stream = Factory.createStream(streamingContext)
      .map(record => record.value)

    val hdfsMaster = Properties.envOrElse("DIGEST_HADOOP_NAMENODE", "hdfs://namenode:8020")
    val serialized = stream.map(value => Vehicle.create(value))
    val tiled = serialized.map(v => Vehicle.makeTiled(v))

    val keyspace = Properties.envOrElse("DIGEST_CASSANDRA_KEYSPACE", "streaming")
    val tileTable = Properties.envOrElse("DIGEST_CASSANDRA_TILE_TABLE", "vehicles_by_tileid")
    val vehicleTable = Properties.envOrElse("DIGEST_CASSANDRA_VEHICLE_TABLE", "vehicles")

    serialized.foreachRDD(rdd => {
      rdd.saveToCassandra(keyspace, vehicleTable)
    })

    stream.map(v => (v.stripPrefix("\"").stripSuffix("\"").split(",")(0), v))
      .saveAsHadoopFiles(hdfsMaster + "/vehiclelocation/data", "txt", classOf[Text], classOf[Text], classOf[TextOutputFormat[String, String]])

    tiled.foreachRDD(rdd => {
      rdd.saveToCassandra(keyspace, tileTable)
    })

    Factory.start(streamingContext)
  }
}
