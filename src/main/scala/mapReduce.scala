import org.apache.spark
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Siori on 15/05/22.
 */
object mapReduce {
  def main(args: Array[String]): Unit = {

    // https://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("WordCount MapReduce Application")

    //https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    //http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/SparkContext.html
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0)) // hdfs://

    val words = input.flatMap(x => x.split(" "))

    val result = words.map(x => (x,1)).reduceByKey((x,y) => x + y)

    val sortResult = result.sortByKey()

    for (r <- sortResult) {
      println(r.toString())
    }

    sc.stop

  }
}