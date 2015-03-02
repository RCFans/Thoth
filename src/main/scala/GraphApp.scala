import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

/**
 * User: Justina Chen
 * Date: 3/2/15
 * Time: 12:38 PM
 */
object GraphApp {

    def main(args: Array[String]) {
        val path = "spark_edges10000"
        val conf = new SparkConf().setMaster("local").setAppName("GraphX Analysis")
        val sc = new SparkContext(conf)

        val graph = GraphLoader.edgeListFile(sc, path)
        val ranks =  graph.pageRank(100).vertices

        val vertex = sc.textFile("spark_vertex").map(line => line.split(",")).map(field => (field(0).toLong, field(1)))

        val ranksByDesc = vertex.join(ranks).map {
            case (id, (desc, rank)) => (desc, rank)
        }

        println(ranksByDesc.top(5)(Ordering.by(_._2)).mkString("\n"))
    }

}
