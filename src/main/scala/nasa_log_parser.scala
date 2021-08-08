import org.apache.spark.sql.functions.{to_date, unix_timestamp}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

object nasa_log_parser extends  App {

  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

  val sc = new SparkContext("local[*]" , "SparkDemo")
  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  import spark.implicits._

  //Create spark RDD from log file.
  val logFile = sc.textFile("/Users/user1/Desktop/assignment-secure-works/NASA_access_log_Jul95")

  val accessLog = logFile.map(parseLogLine)
  val accessDf = accessLog.toDF
  val inputData=prepareData(accessDf)
  inputData.select("host","url" ,"date").groupBy("host","url","date")
    .count()
    .createOrReplaceTempView(" url_counts")
  val test_data = spark.sql("select * from url_counts")

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
   * Parse Log fole and create LogRecord
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
      .withColumn("timestamp_date",unix_timestamp(accessDf.col("timeStamp"), "dd/MMM/yyyy:HH:mm:ss").cast("timestamp"))
      .withColumn("date", to_date($"timestamp_date"))
  }

  /**
   * topN host,url for each date
   */
  def topLogRecord(topN:Int): DataFrame = {

    spark.sql("select * from " +
      "(select date,host,url," +
      "count, Rank() over (partition by date Order by count desc) as rank " +
      "from url_counts) " +
      "where rank <= " + topN)

  }
}
