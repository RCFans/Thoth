import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * User: Justina Chen
 * Date: 3/2/15
 * Time: 11:03 AM
 */
object FreqApp {

    def main(args: Array[String]) {
        val path = "src/main/resources/sparse_sin.csv"
        val conf = new SparkConf().setMaster("local").setAppName("Frequent Analysis")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val dataFile = sc.textFile(path, 2).cache()

        val lines = dataFile.map(_.split(","))

        val titles = lines.first()
        val size = titles.length

        val schemaString = titles.mkString(" ")
        val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, IntegerType , true)))

        val data = lines.zipWithIndex().filter(_._2 > 0).map(_._1)



        val rowRDD = data.map(p => Row(p.map(s => if (s.length > 0) s.toFloat.toInt else 0):_*))

        val schemaRDD = sqlContext.applySchema(rowRDD, schema)

        schemaRDD.registerTempTable("service")

        val results = sqlContext.sql("SELECT C102002 FROM service WHERE C102002 > 0 LIMIT 10")

        results.foreach(println)
    }

}
