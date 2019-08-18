object Model {
  def create(args: String): Model = {
    val splited = args.stripPrefix("\"").stripSuffix("\"").split(",")
    Model(splited(0), splited(1), splited(2).toDouble, splited(3).toDouble)
  }
}


case class Model(
  id: String,
  time: String,
  latitude: Double,
  longitude: Double
)