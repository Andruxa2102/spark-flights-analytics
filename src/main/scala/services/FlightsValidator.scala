package services


import config.Config
import models.Models._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import java.nio.file.Paths
import java.time.LocalDate


@SerialVersionUID(1L)
class FlightsValidator(config: Config) extends Serializable {

  private def parseDate(year: String, month: String, day: String): LocalDate = {
    val formattedMonth = if (month.length == 1) s"0$month" else month
    val formattedDay = if (day.length == 1) s"0$day" else day
    LocalDate.parse(s"$year-$formattedMonth-$formattedDay")
  }

  def getLastProcessedDate(sc: SparkContext): Option[LocalDate] = {

    val dataManager = new DataManager(sc, config)
    val dateString: String = dataManager.loadString(Paths.get(config.cachePath, "last_processed_date.txt").toString)
    scala.util.Try(LocalDate.parse(dateString)).toOption
  }

  def getFlightsData(sc: SparkContext,
                     rdd: RDD[String],
                     airports: Broadcast[Seq[String]]
                    ): (RDD[Flight], LocalDate, LocalDate) = {
    // проверим валидность аэропортов и отбросим записи, уже обработанные за прошлые периоды

    val lastDate: LocalDate = getLastProcessedDate(sc).getOrElse(LocalDate.parse("0001-01-01"))

    val flightsWithDateRDD: RDD[FlightWithDate] = rdd
      .mapPartitionsWithIndex((index, line) => if (index != 0) line else line.drop(1))
      .flatMap { line =>
        val fields = line.split(",", -1)
        val flightDate = parseDate(fields(0), fields(1), fields(2))

        if (airports.value.contains(fields(7)) && airports.value.contains(fields(8)) && flightDate.isAfter(lastDate))
          Some(FlightWithDate(
            flightDate,
            fields(3),
            fields(4),
            fields(7),
            fields(8),
            fields(11),
            fields(22),
            fields(26),
            fields(27),
            fields(28),
            fields(29),
            fields(30)
          ))
        else
          None
      }

    val maxDate: LocalDate = flightsWithDateRDD.map(_.date).max()
    val minDate: LocalDate = flightsWithDateRDD.map(_.date).min()

    val flightsRDD: RDD[Flight] = flightsWithDateRDD.map { flightWithDate =>
      Flight(
        flightWithDate.weekDay, flightWithDate.airline,
        flightWithDate.originAirport, flightWithDate.destinationAirport,
        flightWithDate.departureDelay, flightWithDate.arrivalDelay,
        flightWithDate.airSystemDelay, flightWithDate.securityDelay,
        flightWithDate.airlineDelay, flightWithDate.lateAircraftDelay,
        flightWithDate.weatherDelay
      )
    }
    (flightsRDD, minDate, maxDate)
  }
}
