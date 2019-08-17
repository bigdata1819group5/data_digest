import java.util

import org.apache.kafka.common.serialization.Deserializer
import org.nustaq.serialization.FSTConfiguration

object ModelDeserializer {
  val fst = FSTConfiguration.createDefaultConfiguration()
}

class ModelDeserializer() extends Deserializer[Model] {
  import ModelDeserializer._

  override def deserialize(topic: String, data: Array[Byte]): Model = {
    fst.asObject(data).asInstanceOf[Model]
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

}