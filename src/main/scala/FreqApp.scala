import org.apache.spark.{SparkContext, SparkConf}

/**
 * User: Justina Chen
 * Date: 3/2/15
 * Time: 11:03 AM
 */
object FreqApp {

    def main(args: Array[String]) {
        val path = "freq_sin_noindex.csv"
        val conf = new SparkConf().setMaster("local").setAppName("Frequent Analysis")
        val sc = new SparkContext(conf)
        val data = sc.textFile(path, 2).cache()

        try {
            val lines = data.map(line => line.split(","))
        }
    }

}
