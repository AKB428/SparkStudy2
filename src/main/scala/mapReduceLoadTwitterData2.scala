import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Siori on 15/05/22.
  */
object mapReduceLoadTwitterData2 {
   def main(args: Array[String]): Unit = {

     // https://spark.apache.org/docs/latest/quick-start.html
     val conf = new SparkConf().setAppName("WordCount MapReduce Application")

     //https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
     //http://www.ne.jp/asahi/hishidama/home/tech/scala/spark/SparkContext.html
     conf.setMaster("local[*]")
     val sc = new SparkContext(conf)

     val input = sc.textFile(args(0)) // hdfs://

    // val words = input.flatMap(x => x.split(" "))
    // val filterResult = input.filter(line => line.split(",").head)

     /*
     for (r <- input) {
       println(r.toString())
     }

     for (r <- input) {
       println(r.split(",")(1))
     }*/

     val filterResult = input.filter(line => line.split(",").length > 1)

     val accounts = filterResult.map(x => x.split(",")(1))

     val result = accounts.map(x => (x,1)).reduceByKey((x,y) => x + y)

     val sortResult = result.sortByKey()

     val filterCount = sortResult.filter(x => x._2 > args(1).toInt)

     for (r <- filterCount) {
       println(r.toString())
     }

     sc.stop

   }
 }