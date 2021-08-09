import com.google.inject.matcher.Matchers
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{to_date, unix_timestamp}
import org.apache.spark.sql.SQLContext
import org.scalatest.BeforeAndAfterEach

class nasa_log_parser_test extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with Logging {

  private val master = "local[*]"
  private val appName = "nasa_log_parser_testing"
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
  var spark: SparkSession = _
  var outputTableOrPath: String = _
  var inputDataPath: String = _
  var sqlContext:SQLContext = _
  var sc:SparkContext = _

  override def beforeEach() {
    inputDataPath = System.getProperty("user.dir") + "/src/test/resources/sample_data.txt"
    spark = new SparkSession.Builder().appName(appName).master(master).getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  test("testing_nasa_log_parser")
  {
    val test_data = sc.textFile(inputDataPath)
    val test_accessLog = test_data.map(parseLogLine)
    val accessDf = sqlContext.createDataFrame(test_accessLog)
    val inputData=prepareData(accessDf)
    inputData.select("host","url" ,"date").groupBy("host","url","date")
      .count()
      .createOrReplaceTempView(" url_counts")
    val res = spark.sql("select count from url_counts where date='28/Jul/1995' and host='163.205.53.14'")
    assert(res.count == 1l)
  }

  override def afterEach() {
    spark.stop()
  }

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
}
