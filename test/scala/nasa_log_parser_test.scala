//import org.apache.spark.sql.HashBenchmark.test
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.LocalSparkSession

class nasa_log_parser_test extends SparkFunSuite with LocalSparkSession with BeforeAndAfter
  with BeforeAndAfterAll
  with Eventually
  with Logging {

  private def createSparkContext(conf: Option[SparkConf] = None): SparkContext = {
    new SparkContext(conf.getOrElse(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("test")))
  }

  test("your test")
  {
    val sc = createSparkContext()
  }
}
