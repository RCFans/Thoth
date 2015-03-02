import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * User: Justina Chen
 * Date: 2/27/15
 * Time: 3:57 PM
 */
object StreamingApp {

    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("NetworkWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
