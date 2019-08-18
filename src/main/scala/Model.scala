object Model {
  def create(args: Array[String]): Model = {
    Model(args(0), args(1), args(2).toDouble, args(3).toDouble)
  }
}


case class Model(
  id: String,
  time: String,
  latitude: Double,
  longitude: Double
)