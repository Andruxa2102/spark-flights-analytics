package jobs

import org.apache.spark.sql.SparkSession
import services._
import java.io.File
import java.time.LocalDateTime
import config.{Config, JobConfig}

object FlightProcessor {

  def createSparkSession(jConfig: JobConfig): SparkSession = {

    val builder = SparkSession.builder().appName("FlightAnalyzer")

    val master = if (jConfig.localMode) {
      "local[*]"
    } else {
      jConfig.master
    }

    if (master.nonEmpty) {  
      builder.master(master)
    }

    if (jConfig.localMode) {
      builder
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "true")
    }

    builder.getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    val jobConfig = JobConfig.parseArgs(args)
    val spark = createSparkSession(jobConfig)
    val sc = spark.sparkContext
    val config = Config.createConfig(jobConfig)

    val analyzer = new DataAnalyzer(sc, config, jobConfig)
    val manager = new DataManager(sc, config)

    val metricsTempPath = config.metricsTempPath
    val metricsPath = config.metricsPath
    val cacheTempPath = config.cacheTempPath
    val cachePath = config.cachePath
    val logsPath = config.logsPath

    try {
      manager.deleteDirectory(cacheTempPath)
      manager.deleteDirectory(metricsTempPath)

      new File(cachePath).mkdirs()
      new File(metricsPath).mkdirs()
      new File(logsPath).mkdirs()
      new File(cacheTempPath).mkdirs()
      new File(metricsTempPath).mkdirs()

      val result = analyzer.calcMetrics()
      manager.saveResults(result)
      val firstDate = result.getOrElse("firstDate", null)
      val lastDate = result.getOrElse("lastDate", null)

      manager.addStringToFile(logsPath + "/meta_info.csv",
        s"collected,собраны данные,от $firstDate до $lastDate\n")
      manager.addStringToFile(logsPath + "/meta_info.csv",
        s"processed,анализ произведен,${LocalDateTime.now()}\n")

      manager.renameDirectory(cacheTempPath, cachePath)
      manager.renameDirectory(metricsTempPath, metricsPath)

    } catch {
      case e: Exception =>
        manager.addStringToFile(logsPath + "/meta_info.csv",
          s"error,${e.getMessage},${LocalDateTime.now()}\n")
    } finally {
      spark.stop()
    }
  }
}