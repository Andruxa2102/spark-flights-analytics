package config

import java.nio.file.Paths

case class Config(
  basePath: String,
  cachePath: String,
  cacheTempPath: String,
  inputPath: String,
  metricsPath: String,
  metricsTempPath: String,
  logsPath: String
)

object Config {

  def createConfig(jobConfig: JobConfig): Config = {
    val basePath = getBasePath(jobConfig)

    Config(
      basePath = basePath,
      cachePath = Paths.get(basePath, "cache").toString,
      cacheTempPath = Paths.get(basePath, "cacheTemp").toString,
      metricsPath = Paths.get(basePath, "metrics").toString,
      metricsTempPath = Paths.get(basePath, "metricsTemp").toString,
      inputPath = Paths.get(basePath, "input").toString,
      logsPath = Paths.get(basePath, "logs").toString
    )
  }

  private def getBasePath(jobConfig: JobConfig): String = {

    if (jobConfig.basePath != null && jobConfig.basePath.nonEmpty) {
      // если basePath указан
      jobConfig.basePath
    } else if (jobConfig.localMode) {
      // запуск локально
      java.nio.file.Paths.get("src/main/resources")
        .toAbsolutePath
        .normalize()
        .toString
    } else {
      // Кластер + без basePath - возвращаем ошибку
      throw new IllegalArgumentException(
        s"Base path is required for cluster mode. " +
          s"Master: ${jobConfig.master}, " +
          s"Local mode: ${jobConfig.localMode}"
      )
    }
  }
}