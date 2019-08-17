import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    val Array(kafkaHost, topics, cassandraHost) = args

    val schema = new StructType()
      .add("id",StringType)
      .add("latitude",DoubleType)
      .add("longitude",DoubleType)
      .add("time",StringType)

    val spark = SparkSession
      .builder
      .appName("DigestData")
      .config("spark.cassandra.connection.host", cassandraHost)
      .getOrCreate()

    import spark.implicits._

    val dataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", topics)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String]

    dataFrame.printSchema()
    val jsons = dataFrame.select(from_json($"value", schema) as "data").select("data.*")

    val query = jsons.writeStream
      .foreachBatch { (batchDF, _) =>
        batchDF
          .write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "sparkdata")
          .option("table", "detaillocation")
          .mode("append")
          .save
      }.start

    val debug = jsons.writeStream
        .outputMode("append")
        .format("console")
        .start

    debug.awaitTermination
    query.awaitTermination
  }
}
