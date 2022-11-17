package word_count

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

case class Jane(word: String)

object Jane {
    val conf: SparkConf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("word count")
    val sc: SparkContext = new SparkContext(conf)
    val janeRdd: RDD[Jane] = sc.textFile("C:\\Users\\snapp\\Documents\\Courses\\spark-scala\\resources\\JaneAusten.txt")
                            .flatMap(_.split(" "))
                            .map(w => preprocess(w))
                            .map(e => Jane(e)).persist()

    def preprocess(word: String): String = {
        val pattern = new Regex("[a-zA-Z0-9\\s]")
        (pattern findAllIn word).mkString("")
    }

    def count_words = {
        janeRdd.toDF().groupBy(identity).count()//.sort(desc("count")).show()
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        println(Jane.count_words)
    }
}
