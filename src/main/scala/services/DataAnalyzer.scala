package services

import config.{Config, JobConfig}
import models.Models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import java.nio.file.Paths
import java.time.LocalDate
import scala.util.Try

@SerialVersionUID(1L)
class DataAnalyzer(
                    sc: SparkContext,
                    config: Config,
                    jobConfig: JobConfig) extends Serializable {

  private val validator = new FlightsValidator(config)
  private val dataManager = new DataManager(sc, config)
  private val cachePath: String = config.cachePath + "/"
  private val inputPath: String = config.inputPath + "/"

  def calcMetrics(): Map[String, Any] = {

    val airlines = sc.broadcast(dataManager.loadDictionaries(Paths.get(inputPath, "airlines.csv").toString))
    val airportsMaps = dataManager.loadDictionaries(Paths.get(inputPath, "airports.csv").toString)
    val airportIds = sc.broadcast(airportsMaps.keys.toSeq)
    val airports = sc.broadcast(airportsMaps)

    val rawFlightsRDD: RDD[String] = sc.textFile(Paths.get(inputPath, jobConfig.inputFile).toString)
    val (unpersistedFlightsRDD, firstDate, lastDate): (RDD[Flight], LocalDate, LocalDate) = validator.getFlightsData(sc, rawFlightsRDD, airportIds)
    val flightsRDD: RDD[Flight] = unpersistedFlightsRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val noDelayDepartureRDD:RDD[(String, String, String)] = flightsRDD.flatMap { i =>
      if (i.departureDelay == "0")
        Some((i.airline, i.originAirport, i.destinationAirport))
      else
        None
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val (topTenAirportsMetrics, topTenAirportsCache) = getTopTenAirports(flightsRDD, airports)
    val (topTenAirlinesMetrics, topTenAirlinesCache) = getTopTenAirlines(flightsRDD, airlines)
    val (topTenAirportsPerAvialinesMetrics, topTenAirportsPerAvialinesCache) = getTopTenAirportsPerAvialines(noDelayDepartureRDD, airports, airlines)
    val (topTenDestinationsMetrics, topTenDestinationsCache) = getTopTenDestinations(noDelayDepartureRDD, airports)
    val (weekdaysOrderbyDelaysMetrics, weekdaysOrderbyDelaysCache) = getWeekdaysOrderbyDelays(flightsRDD)
    val (delaysPerReasonsMetrics, delaysPerReasonsCache) = getDelaysPerReasons(flightsRDD)
    val (percentDelayPerReasonsMetrics, percentDelayPerReasonsCache) = getPercentDelayPerReasons(flightsRDD)

    val result = Map(
      "topTenAirportsMetrics" -> topTenAirportsMetrics,
      "topTenAirportsCache" -> topTenAirportsCache,
      "topTenAirlinesMetrics" -> topTenAirlinesMetrics,
      "topTenAirlinesCache" -> topTenAirlinesCache,
      "topTenAirportsPerAvialinesMetrics" -> topTenAirportsPerAvialinesMetrics,
      "topTenAirportsPerAvialinesCache" -> topTenAirportsPerAvialinesCache,
      "topTenDestinationsMetrics" -> topTenDestinationsMetrics,
      "topTenDestinationsCache" -> topTenDestinationsCache,
      "weekdaysOrderbyDelaysMetrics" -> weekdaysOrderbyDelaysMetrics,
      "weekdaysOrderbyDelaysCache" -> weekdaysOrderbyDelaysCache,
      "delaysPerReasonsMetrics" -> delaysPerReasonsMetrics,
      "delaysPerReasonsCache" -> delaysPerReasonsCache,
      "percentDelayPerReasonsMetrics" -> percentDelayPerReasonsMetrics,
      "percentDelayPerReasonsCache" -> percentDelayPerReasonsCache,
      "lastDate" -> lastDate,
      "firstDate" -> firstDate
    )

    result
  }

  def getTopTenAirports(rdd: RDD[Flight],
                        broadcastAirports: Broadcast[Map[String, String]],
                        isAsc: Boolean = jobConfig.topTenAirportsSortAsc
                       ): (Array[(String, Int)], RDD[(String, Int)]) = {
    // возвращаем 1 метрику + накопленную статистику за весь период

    val updatedRDD: RDD[(String, Int)] = rdd.map(i => (i.originAirport, 1))

    val cachedRDD: RDD[(String, Int)] = dataManager.loadRDD2(Paths.get(cachePath, "topTenAirportsCache/*").toString)

    val unsortedRDD: RDD[(String, Int)] = updatedRDD
      .union(cachedRDD)
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val sortedRDD: RDD[(String, Int)] = if (isAsc)
      unsortedRDD.sortBy(_._2)
    else
      unsortedRDD.sortBy(-_._2)

    val metric: Array[(String, Int)] = sortedRDD
      .sortBy(-_._2)
      .take(10)
      .flatMap { case (key, topTenValue) =>
        broadcastAirports.value.get(key).map { airValue =>
          (airValue, topTenValue)
        }
      }

    (metric, unsortedRDD)
  }

  def getTopTenAirlines(rdd: RDD[Flight],
                        broadcastAirlines: Broadcast[Map[String, String]],
                        isAsc: Boolean = jobConfig.topTenAirlinesSortAsc
                       ): (Array[(String, Int)], RDD[(String, Int)]) = {
    // возвращаем 2 метрику + накопленную статистику за весь период

    val updatedRDD: RDD[(String, Int)] = rdd.flatMap { i =>
      if (i.departureDelay == "0" && i.arrivalDelay == "0")
        Some(i.airline, 1)
      else
        None
    }

    val cachedRDD: RDD[(String, Int)] = dataManager.loadRDD2(Paths.get(cachePath, "topTenAirlinesCache/*").toString)

    val unsortedRDD: RDD[(String, Int)] = updatedRDD
      .union(cachedRDD)
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val sortedRDD: RDD[(String, Int)] = if (isAsc)
      unsortedRDD.sortBy(_._2)
    else
      unsortedRDD.sortBy(-_._2)

    val metricRDD: Array[(String, Int)] = sortedRDD
      .take(10)
      .flatMap { case (key, topTenValue) =>
        broadcastAirlines.value.get(key).map { airValue =>
          (airValue, topTenValue)
        }
      }

    (metricRDD, unsortedRDD)
  }

  def getTopTenAirportsPerAvialines(rdd: RDD[(String, String, String)],
                                    broadcastAirports: Broadcast[Map[String, String]],
                                    broadcastAirlines: Broadcast[Map[String, String]],
                                    isAsc: Boolean = jobConfig.topAirportsPerAvialinesSortAsc
                                   ): (RDD[(String, String, Int)], RDD[((String, String), Int)]) = {
    // возвращаем 3 метрику + накопленную статистику за весь период

    val updatedRDD: RDD[((String, String), Int)] = rdd
      .map { case (airline, origin, _) =>
        ((origin, airline), 1)
      }

    val cachedRDD: RDD[((String, String), Int)] = dataManager.loadRDD3(Paths.get(cachePath, "topTenAirportsPerAvialinesCache/*").toString)

    val unsortedRDD: RDD[((String, String), Int)] = updatedRDD
      .union(cachedRDD)
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val groupedRDD: RDD[(String, Iterable[(String, Int)])] = unsortedRDD
      .map { case ((origin, airline), quantity) =>
        (origin, (airline, quantity))
      }
      .groupByKey()

    // выбор из топ-10 или даун-10
    val sortedRDD: RDD[(String, String, Int)] = if (isAsc)
      groupedRDD
        .flatMap { case (origin, airlines) =>
          airlines.toSeq
            .sortBy(_._2)
            .take(10)
            .map { case (airlines, quantity) =>
              (origin, airlines, quantity)
            }
        }
    else
      groupedRDD
        .flatMap { case (origin, airlines) =>
          airlines.toSeq
            .sortBy(-_._2)
            .take(10)
            .map { case (airlines, quantity) =>
              (origin, airlines, quantity)
            }
        }

    // джойны для замены ИДюков на имена
    val metricRDD: RDD[(String, String, Int)] = sortedRDD
      .map { case (origin, airline, quantity) => (origin, (airline, quantity)) }
      .flatMap { case (key, topTenValue) =>
        broadcastAirports.value.get(key).map { airValue =>
          (airValue, topTenValue)
        }
      }
      .map { case (origin, (airline, quantity)) => (airline, (origin, quantity)) }
      .flatMap { case (key, topTenValue) =>
        broadcastAirlines.value.get(key).map { airValue =>
          (airValue, topTenValue)
        }
      }
      .map { case (airline, (origin, quantity)) => (origin, airline, quantity) }

    (metricRDD, unsortedRDD)
  }

  def getTopTenDestinations(rdd: RDD[(String, String, String)],
                            broadcastAirports: Broadcast[Map[String, String]],
                            isAsc: Boolean = jobConfig.topTenDestinationsSortAsc
                           ): (RDD[(String, String, Int)], RDD[((String, String), Int)]) = {
    // возвращаем 4 метрику + накопленную статистику за весь период

    val updatedRDD: RDD[((String, String), Int)] = rdd
      .map { case (_, origin, destination) =>
        ((origin, destination), 1)
      }

    val cachedRDD: RDD[((String, String), Int)] = dataManager.loadRDD3(Paths.get(cachePath, "topTenDestinationsCache/*").toString)

    val unsortedRDD: RDD[((String, String), Int)] = updatedRDD
      .union(cachedRDD)
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val groupedRDD: RDD[(String, Iterable[(String, Int)])] = unsortedRDD
      .map { case ((origin, destination), quantity) =>
        (origin, (destination, quantity))
      }
      .groupByKey()

    // выбор из топ-10 или даун-10
    val sortedRDD: RDD[(String, String, Int)] = if (isAsc)
      groupedRDD
        .flatMap { case (origin, destinations) =>
          destinations.toSeq
            .sortBy(_._2)
            .take(10)
            .map { case (destinations, quantity) =>
              (origin, destinations, quantity)
            }
        }
    else
      groupedRDD
        .flatMap { case (origin, destinations) =>
          destinations.toSeq
            .sortBy(-_._2)
            .take(10)
            .map { case (destinations, quantity) =>
              (origin, destinations, quantity)
            }
        }

    // джойны для замены ИДюков на имена
    val metricRDD: RDD[(String, String, Int)] = sortedRDD
      .map { case (origin, destination, quantity) => (origin, (destination, quantity)) }
      .flatMap { case (key, topTenValue) =>
        broadcastAirports.value.get(key).map {airValue =>
          (airValue, topTenValue)
        }
      }
      .map { case (origin, (destination, quantity)) => (destination, (origin, quantity)) }
      .flatMap { case (key, topTenValue) =>
        broadcastAirports.value.get(key).map {airValue =>
          (airValue, topTenValue)}
      }
      .map { case (destination, (origin, quantity)) => (origin, destination, quantity)}

    (metricRDD, unsortedRDD)
  }

  def getWeekdaysOrderbyDelays(rdd: RDD[Flight]): (Array[(String, Int)], RDD[(String, Int)]) = {
    // возвращаем 5 метрику + накопленную статистику за весь период

    val updatedRDD: RDD[(String, Int)] = rdd
      .flatMap { i =>
        if (i.arrivalDelay == "0")
          Some(i.weekDay, 1)
        else
          None
      }

    val cachedRDD: RDD[(String, Int)] = dataManager.loadRDD2(Paths.get(cachePath, "weekdaysOrderbyDelaysCache/*").toString)

    val unsortedRDD: RDD[(String, Int)] = updatedRDD
      .union(cachedRDD)
      .reduceByKey(_ + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val metric: Array[(String, Int)] = unsortedRDD
      .sortBy(-_._2)
      .collect()

    (metric, unsortedRDD)
  }

  def getDelaysPerReasons(rdd: RDD[Flight]): (Array[(String, Int)], (Int, Int, Int, Int, Int)) = {
    // возвращаем 6 метрику + накопленную статистику за весь период

    val preDelayValues: RDD[(Int, Int, Int, Int, Int)] = rdd
      .flatMap { i =>
        if (i.airSystemDelay != "" || i.securityDelay != "" || i.airlineDelay != "" || i.lateAircraftDelay != "" || i.weatherDelay != "")
          Some(
            if (i.airSystemDelay == "" || i.airSystemDelay == "0") 0 else 1,
            if (i.securityDelay == "" || i.securityDelay == "0") 0 else 1,
            if (i.airlineDelay == "" || i.airlineDelay == "0") 0 else 1,
            if (i.lateAircraftDelay == "" || i.lateAircraftDelay == "0") 0 else 1,
            if (i.weatherDelay == "" || i.weatherDelay == "0") 0 else 1)
        else
          None
      }

    val delayValues: (Int, Int, Int, Int, Int) = if (preDelayValues.isEmpty()) {
      (0, 0, 0, 0, 0)
    } else {
      preDelayValues.reduce { (arr1, arr2) =>
        (
          arr1._1 + arr2._1,
          arr1._2 + arr2._2,
          arr1._3 + arr2._3,
          arr1._4 + arr2._4,
          arr1._5 + arr2._5
        )
      }
    }

    val loaded: Array[String] = dataManager
      .loadString(Paths.get(cachePath, "delaysPerReasonsCache.csv").toString)
      .split(",")

    val cached: Option[(Int, Int, Int, Int, Int)] = Try((
      loaded(0).toInt,
      loaded(1).toInt,
      loaded(2).toInt,
      loaded(3).toInt,
      loaded(4).trim.toInt
    )).toOption

    val preMetrics: (Int, Int, Int, Int, Int) = cached match {
      case Some(value) => (
        delayValues._1 + value._1,
        delayValues._2 + value._2,
        delayValues._3 + value._3,
        delayValues._4 + value._4,
        delayValues._5 + value._5)
      case None => delayValues
    }

    val metrics: Array[(String, Int)] = Array(
      ("AIR_SYSTEM_DELAY", preMetrics._1),
      ("SECURITY_DELAY", preMetrics._2),
      ("AIRLINE_DELAY", preMetrics._3),
      ("LATE_AIRCRAFT_DELAY", preMetrics._4),
      ("WEATHER_DELAY", preMetrics._5)
    )

    (metrics, preMetrics)
  }

  def getPercentDelayPerReasons(rdd: RDD[Flight]): (Array[(String, Double)], (Int, Int, Int, Int, Int)) = {
    // возвращаем 7 метрику + накопленную статистику за весь период

    val preDelayValues: RDD[(Int, Int, Int, Int, Int)] = rdd
      .flatMap { i =>
        if (i.airSystemDelay != "" || i.securityDelay != "" || i.airlineDelay != "" || i.lateAircraftDelay != "" || i.weatherDelay != "")
          Some(
            if (i.airSystemDelay == "" || i.airSystemDelay == "0") 0 else i.airSystemDelay.toInt,
            if (i.securityDelay == "" || i.securityDelay == "0") 0 else i.securityDelay.toInt,
            if (i.airlineDelay == "" || i.airlineDelay == "0") 0 else i.airlineDelay.toInt,
            if (i.lateAircraftDelay == "" || i.lateAircraftDelay == "0") 0 else i.lateAircraftDelay.toInt,
            if (i.weatherDelay == "" || i.weatherDelay == "0") 0 else i.weatherDelay.toInt)
        else
          None
      }

    val delayValues: (Int, Int, Int, Int, Int) = if (preDelayValues.isEmpty()) {
      (0, 0, 0, 0, 0)
    } else {
      preDelayValues.reduce { (arr1, arr2) =>
        (
          arr1._1 + arr2._1,
          arr1._2 + arr2._2,
          arr1._3 + arr2._3,
          arr1._4 + arr2._4,
          arr1._5 + arr2._5
        )
      }
    }

    val loaded: Array[String] = dataManager
      .loadString(Paths.get(cachePath, "percentDelayPerReasonsCache.csv").toString)
      .split(",")

    val cached: Option[(Int, Int, Int, Int, Int)] = Try((
      loaded(0).toInt,
      loaded(1).toInt,
      loaded(2).toInt,
      loaded(3).toInt,
      loaded(4).trim.toInt
    )).toOption

    val preMetrics: (Int, Int, Int, Int, Int) = cached match {
      case Some(value) => (
        delayValues._1 + value._1,
        delayValues._2 + value._2,
        delayValues._3 + value._3,
        delayValues._4 + value._4,
        delayValues._5 + value._5)
      case None => delayValues
    }

    val fullDelay: Int = preMetrics._1 + preMetrics._2 + preMetrics._3 + preMetrics._4 + preMetrics._5

    val metrics: Array[(String, Double)] = Array(
      ("AIR_SYSTEM_DELAY", Math.round(10000.0 * preMetrics._1 / fullDelay) / 100.0),
      ("SECURITY_DELAY", Math.round(10000.0 * preMetrics._2 / fullDelay) / 100.0),
      ("AIRLINE_DELAY", Math.round(10000.0 * preMetrics._3 / fullDelay) / 100.0),
      ("LATE_AIRCRAFT_DELAY", Math.round(10000.0 * preMetrics._4 / fullDelay) / 100.0),
      ("WEATHER_DELAY", Math.round(10000.0 * preMetrics._5 / fullDelay) / 100.0)
    )

    (metrics, preMetrics)
  }
}
