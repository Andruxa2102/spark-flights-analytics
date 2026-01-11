<div align="center">
  Приложение для анализа данных о авиаперелетах
</div>


## Возможности

### Особенности
```text
- Apache Spark              - фреймворк, используемый для расчетов;
- Язык Scala                - использован для написания кода приложения;
- RDD API                   - здесь для расчетов метрик применяются Коллекции RDD;
- Инкрементальная обработка - только новые данные с момента последнего запуска;
- Транзакционность          - или выполняем полностью, или обрываем выполнение и пишем ошибку в лог;
- Гибкость запуска          - работает как локально, так и на кластере Spark, определяется подачей нужных ключей;
- Настраиваемые метрики     - в метриках **"Топ-10 ..."** реализована возможность поменять сортировку;
```

### Основные метрики
```text
1. topTenAirportsMetrics             - Топ-10 самых популярных аэропортов по количеству совершаемых полетов**
2. topTenAirlinesMetrics             - Топ-10 авиакомпаний, вовремя выполняющих рейсы
3. topTenAirportsPerAvialinesMetrics - Топ-10 перевозчиков для каждого аэропорта вылета на основании вовремя совершенного вылета
4. topTenDestinationsMetrics         - Топ-10 аэропортов назначения для каждого аэропорта вылета на основании вовремя совершенного вылета**
5. weekdaysOrderbyDelaysMetrics      - дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни**
6. delaysPerReasonsMetrics           - Статистика задержек по причинам
7. percentDelayPerReasonsMetrics     - Процент от общего количества минут задержки рейсов
```

## Архитектура проекта

```text
flight-analyzer/ 
├─ src/
│  ├─ main/
│  │  ├─ scala/
│  │  │  ├─ com/example/
│  │  │  │  └─ FlightAnalyzer.scala         # Точка входа для Spark Submit
│  │  │  ├─ jobs/
│  │  │  │  └── FlightProcessor.scala       # Основной рабочий класс
│  │  │  ├─ services/                       # Бизнес-логика
│  │  │  │  ├── DataAnalyzer.scala          # Класс с реализацией аналитики
│  │  │  │  ├── DataManager.scala           # Класс для взаимодействия с диском
│  │  │  │  └── FlightsValidator.scala      # Класс для валидации данных по полетам
│  │  │  ├─ models/                         # Модели данных
│  │  │  │  └── Models.scala
│  │  │  └─ config/                         # Конфигурация
│  │  │     ├── Config.scala                # Конфигурация приложения
│  │  │     └── JobConfig.scala             # Конфигурация запуска
│  │  ├─ resources/
│  │  │  ├──  cacheTemp/                    # Временный кэш
│  │  │  └──  metricsTemp/                  # Временные результаты вычислений
│  │  ├─ input/                             # Входные данные
│  │  │  ├── flights.csv
│  │  │  ├── airlines.csv
│  │  │  └── airports.csv
│  │  ├─ cache/                             # Инкрементальный кэш
│  │  ├─ metrics/                           # Результаты вычислений
│  │  └─ logs/                              # Логи выполнения
│  └─ test/                                 # Тесты
├─ project/                                 # Конфигурационные файлы проекта
│  └─ build.properties
├─ build.sbt
└─ README.md
```

## Быстрый старт

### Требования
- Java 8+
- Scala 2.xx
- Apache Spark 3.x
- SBT или Maven

### Локальный запуск

### 1. Клонировать репозиторий
git clone https://github.com/your-username/spark-flights-analytics.git
cd spark-flights-analytics

### 2. Собрать проект (SBT)
sbt clean compile

### 3. Запустить
spark-submit --class com.example.FlightAnalyzer target/scala-2.13/flight-analyzer.jar

spark-submit \
  --class com.example.FlightAnalyzer \
  --master local[*] \
  target/scala-2.13/flights-analyzer.jar
  
  
## Ключи для запуска приложения

- **--input-file fileName.csv**         - указать имя файла для подачи данных о полетах в приложение
- **--local**                           - запуск локально, используя путь src/main/resources для хранения данных
- **--master param**                    - параметр, где запускаем приложение:  
&nbsp;&nbsp;&nbsp;&nbsp;- local[N]                      - запуск на локальной машине, используя N ядер;  
&nbsp;&nbsp;&nbsp;&nbsp;- local[*]                      - запуск на локальной машине, используя все ядра;  
&nbsp;&nbsp;&nbsp;&nbsp;- yarn                          - запуск на кластере с диспетчером YARN;  
&nbsp;&nbsp;&nbsp;&nbsp;- spark://host:port             - запуск на кластере с диспетчером Spark Standalone с указанием адреса и порта;  
&nbsp;&nbsp;&nbsp;&nbsp;- mesos://host:port             - запуск на кластере с диспетчером Mesos с указанием адреса и порта;  
- **--base-path**                       - путь к файловому хранилищу, месту расположения входных файлов и результатов;
- **--topTenAirportsSortDesc**          - сменить порядок сортировки ASC -> DESC в метрике **topTenAirportsMetrics**
- **--topTenAirlinesSortDesc**          - сменить порядок сортировки ASC -> DESC в метрике **topTenAirlinesMetrics**
- **--topAirportsPerAvialinesSortDesc** - сменить порядок сортировки ASC -> DESC в метрике **topTenAirportsPerAvialinesMetrics**
- **--topTenDestinationsSortDesc**      - сменить порядок сортировки ASC -> DESC в метрике **topTenDestinationsMetrics**

## Источники данных

- **airlines.csv** - справочник авиакомпании
- **airports.csv** - справочник аэропорты
- **flights.csv**  - данные о полетах, можно использовать другой файл, если ввести с ключом **--input-file**
