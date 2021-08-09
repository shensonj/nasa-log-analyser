import org.apache.spark.sql.functions.{to_date, unix_timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

object nasa_log_parser extends  App with Logging {

  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
  var inputDataPath: String = _
  var outputTableOrPath: String = _
  var master: String = _
  var noOfPartitions: Int = _
  var topN: Int  = _

  if (args.length eq 5)
  {
    inputDataPath = args(0)
    outputTableOrPath = args(1)
    master = args(2)
    noOfPartitions = args(3).toInt
    topN = args(4).toInt
  }
  else if (args.length eq 0) log.info("No paramters given, hence defaults assumed!! " + "\n inputDataPath: "
    + inputDataPath + "\n outputTableOrPath: " + outputTableOrPath + "\n master: " + master + "\n noOfPartitions: "
    + noOfPartitions + "\n topN: " + topN)
  else
  {
    log.warn( "Invalid input parameters!!")
    log.warn("Usage: inputDataPath outputTableOrPath master noOfPartitions topN")
    System.exit(1)
  }
  val sc = new SparkContext("local[*]" , "nasa_log_parser_app")
  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  import spark.implicits._

  //Create spark RDD from log file.
  val logFile = sc.textFile(inputDataPath)

  val accessLog = logFile.map(parseLogLine)
  val accessDf = accessLog.toDF
  val inputData=prepareData(accessDf)
  inputData.select("host","url" ,"date").groupBy("host","url","date")
    .count()
    .createOrReplaceTempView(" url_counts")

  /**
   * Calculate topN host & url per each day
   */
  val result = topLogRecord(10)

  /**
   * Printing topN result
   */
  result.foreach(f=>{
     println(f)
  })

  /**
   * Parse Log file and create LogRecord
   */
  def parseLogLine(log: String) :
  LogRecord = {
    log match {  case PATTERN(host, group2, group3,timeStamp,group5,url,group7,httpCode,group8) => LogRecord(s"$host",s"$timeStamp",s"$url", s"$httpCode".toInt)
    case _ => LogRecord("Empty", "", "",  -1 )}
  }

  /**
   * Prepare the dataset
   */
  def prepareData (input: DataFrame): DataFrame = {
    input.select($"*").filter($"host" =!= "Empty")
      .withColumn("timestamp_date",unix_timestamp(input.col("timeStamp"), "dd/MMM/yyyy:HH:mm:ss").cast("timestamp"))
      .withColumn("date", to_date($"timestamp_date"))
  }

  /**
   * Find topN host,url for each date
   */
  def topLogRecord(topN:Int): DataFrame = {

    spark.sql("select * from " +
      "(select date,host,url," +
      "count, Rank() over (partition by date Order by count desc) as rank " +
      "from url_counts) " +
      "where rank <= " + topN)

  }
}
