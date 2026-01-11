package config

case class JobConfig(
  inputFile: String,
  topTenAirportsSortAsc: Boolean,
  topTenAirlinesSortAsc: Boolean,
  topAirportsPerAvialinesSortAsc: Boolean,
  topTenDestinationsSortAsc: Boolean,
  localMode: Boolean = true,
  master: String,
  basePath: String
)

object JobConfig {

  def parseArgs(args: Array[String]): JobConfig = {

    var inputFile = "flights.csv"
    var master = ""
    var basePath = ""

    args.sliding(2, 1).foreach {
      case Array("--input-file", value) =>
        inputFile = value
      case Array("--master", value) =>
        master = value
      case Array("--base-path", value) =>
        basePath = value
    }

    val argsMap = args.flatMap {
      case "--topTenAirportsSortDesc" => Some("topTenAirportsSortAsc" -> false)
      case "--topTenAirlinesSortDesc" => Some("topTenAirlinesSortAsc" -> false)
      case "--topAirportsPerAvialinesSortDesc" => Some("topAirportsPerAvialinesSortAsc" -> false)
      case "--topTenDestinationsSortDesc" => Some("topTenDestinationsSortAsc" -> false)
      case "--local" => Some("localMode" -> true)
      case _ => None
    }.toMap

    JobConfig(
      inputFile = inputFile,
      topTenAirportsSortAsc = argsMap.getOrElse("topTenAirportsSortAsc", true),
      topTenAirlinesSortAsc = argsMap.getOrElse("topTenAirlinesSortAsc", true),
      topAirportsPerAvialinesSortAsc = argsMap.getOrElse("topAirportsPerAvialinesSortAsc", true),
      topTenDestinationsSortAsc = argsMap.getOrElse("topTenDestinationsSortAsc", true),
      localMode = argsMap.getOrElse("localMode", true),
      master = master,
      basePath = basePath
    )
  }
}