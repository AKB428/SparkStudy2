import org.apache.spark
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Siori on 15/05/22.
 */
object sample1 {
  def main(args: Array[String]): Unit = {

    // https://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("WordCount Application")

    //https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    //http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/SparkContext.html
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0)) // hdfs://
    val filterResult = textFile.filter(line => line.contains(args(1)))
    // Count all the errors
    println(filterResult.count())


    // Count errors mentioning MySQL
    //filterResult.filter(line => line.contains("MySQL")).count()
    // Fetch the MySQL errors as an array of strings
    //filterResult.filter(line => line.contains("MySQL")).collect()

    sc.stop

  }
}