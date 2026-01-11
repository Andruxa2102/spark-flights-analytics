package services

import config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Try

@SerialVersionUID(1L)
class DataManager(sc: SparkContext, config: Config) extends Serializable {

  private val metricsTempPath = config.metricsTempPath
  private val cacheTempPath = config.cacheTempPath + "/"
  private val fs = FileSystem.get(sc.hadoopConfiguration)

  def loadDictionaries(filePath: String): Map[String, String] = {
    // загрузка справочников airlines и airports

    sc.textFile(filePath)
      .mapPartitionsWithIndex((index, line) => if (index != 0) line else line.drop(1))
      .map { line =>
        val fields = line.split(",")
        (fields(0), fields(1))
      }
      .collect()
      .toMap
  }

  def saveResults(metrics: Map[String, Any]): String = {

    val result: Iterable[Any] = metrics.collect { case (k, v) =>
      k match {
        case "lastDate" => stringToFile(Paths.get(cacheTempPath, "last_processed_date.txt").toString, v.toString)
        case "firstDate" =>
        case k if k.contains("Metrics") && v.isInstanceOf[Array[_]] =>
          arrayToFile(Paths.get(metricsTempPath, k + ".csv").toString, v.asInstanceOf[Array[Any]])
        case k if k.contains("Metrics") && v.isInstanceOf[RDD[_]] =>
          arrayToFile(Paths.get(metricsTempPath, k + ".csv").toString, v.asInstanceOf[RDD[Any]].collect())
        case k if k.contains("Cache") && v.isInstanceOf[RDD[_]] =>
          rddToFile(Paths.get(cacheTempPath, k).toString, v.asInstanceOf[RDD[Product]])
        case k if k.contains("Cache") && v.isInstanceOf[(Int, Int, Int, Int, Int)] =>
          tupleToFile(Paths.get(cacheTempPath, k + ".csv").toString, v.asInstanceOf[(Int, Int, Int, Int, Int)])
        case _ => println(s"Ошибка, ключ неизвестен: $k")
      }
    }
    result.mkString("\n")
  }

  def tupleToFile(fileName: String, tpl: (Int, Int, Int, Int, Int)): Unit = {

    val saveString: String = s"${tpl._1},${tpl._2},${tpl._3},${tpl._4},${tpl._5}"
    stringToFile(fileName, saveString)
  }

  def stringToFile(fileName: String, str: Any): Unit = {

    val writer = new PrintWriter(fileName)
    writer.print(str.toString)
    writer.close()
  }

  def arrayToFile(fileName: String, arr: Array[Any]): Unit = {
    val writer = new java.io.PrintWriter(new java.io.File(fileName))
    arr.foreach {
      case (field1: String, field2: Int) =>
        writer.println(s"$field1,$field2")
      case (field1: String, field2: String, field3: Int) =>
        writer.println(s"$field1,$field2,$field3")
      case ((field1: String, field2: String), field3: Int) =>
        writer.println(s"$field1,$field2,$field3")
      case other =>
        writer.println(other.toString.filterNot("()".contains(_)))
    }
    writer.close()
  }

  def rddToFile(fileName: String, rdd: RDD[Product]): Unit = {

    rdd.map {
      i => i.productIterator.mkString(",")
    }
      .saveAsTextFile(fileName)
  }

  def loadString(fileName: String): String = {

    val filePath = Paths.get(fileName)
    if (Files.exists(filePath)) {
      Files.readString(filePath, StandardCharsets.UTF_8)
    } else {
      ""
    }
  }

  def loadRDD2(fileName: String): RDD[(String, Int)] = {

    try {
      val rdd = sc.textFile(fileName)
      if (rdd.isEmpty()) sc.emptyRDD[(String, Int)]
      else {
        rdd.map { line =>
          val fields = line.split(",")
          (fields(0), fields(1).toInt)
        }
      }
    } catch {
      case _: Exception =>
        sc.emptyRDD[(String, Int)]
    }
  }

  def loadRDD3(fileName: String): RDD[((String, String), Int)] = {

    try {
      val rdd = sc.textFile(fileName)
      if (rdd.isEmpty()) sc.emptyRDD[((String, String), Int)]
      else {
        rdd.map { i =>
          val item = i.split("[,()]")
          ((item(1), item(2)), item(4).toInt)
        }
      }
    } catch {
      case _: Exception =>
        sc.emptyRDD[((String, String), Int)]
    }
  }

  def deleteDirectory(fileName: String): Unit = {

    Try {
      val hadoopPath = new Path(fileName)

      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
      }
    }
  }

  def renameDirectory(from: String, to: String): Unit = {

    val source = new Path(from)
    val target = new Path(to)

    if (fs.exists(source)) {
      if (fs.exists(target)) {
        fs.delete(target, true)
      }
      fs.rename(source, target)
    }
  }

  def addStringToFile(fileName: String, str: String): Unit = {

    val filePath = Paths.get(fileName)
    Files.writeString(filePath, str, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

}
