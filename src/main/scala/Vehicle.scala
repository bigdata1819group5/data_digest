import tiler.TileCalc

object Vehicle {
  def create(args: String): Vehicle = {
    val splited = args.stripPrefix("\"").stripSuffix("\"").split(",")
    Vehicle(splited(0), splited(1), splited(2).toDouble, splited(3).toDouble)
  }
  def makeTiled(vehicle: Vehicle): TiledVehicle = {
    TiledVehicle(
      vehicle.id,
      vehicle.time,
      vehicle.latitude,
      vehicle.longitude,
      TileCalc.convertLatLongToQuadKey(vehicle.latitude, vehicle.longitude)
    )
  }
}


case class Vehicle(
  id: String,
  time: String,
  latitude: Double,
  longitude: Double
)

case class TiledVehicle(
  id: String,
  time: String,
  latitude: Double,
  longitude: Double,
  quadKey: String
)