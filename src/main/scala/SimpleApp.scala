/**
 * User: Justina Chen
 * Date: 2/26/15
 * Time: 12:53 PM
 */

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object SimpleApp {

    def main(args: Array[String]) {
        val logFile = "/usr/local/share/spark/README.md"
        val conf = new SparkConf().setAppName("Simple Application")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile, 2).cache()
        try {
//            val numAs = countA(logData, "a")
//            val numBs = countA(logData, "b")
//            val totalLines = countLines(logData)
            val wordsCount = countLineKeys(logData)
//            println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
//            println("Total lines are: %d".format(totalLines))
            wordsCount.sortByKey().collect().foreach(pair => println(pair._1 + ": " + pair._2))
        } catch {
            case ex: InvalidInputException => println("can't find the file.")
            case ex: Exception => println(ex.getMessage)
        }
    }

    def countA(rdd: RDD[String], key: String) :Float = {
        rdd.filter(line => line.contains(key)).count()
    }

    def countLines(rdd: RDD[String]) :Int = {
        val lineLength = rdd.map(line => line.split(" ").length)
        lineLength.reduce((a, b) => (a + b))
    }

    def countLineKeys(rdd: RDD[String]) :RDD[(String, Int)] = {
        rdd.flatMap(line => line.split(" ").map(word => (word.toLowerCase(), 1))).reduceByKey((a, b) => (a + b))
    }

}
