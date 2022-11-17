package word_count

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

case class Jane(word: String)

object Jane {
    val conf: SparkConf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("word count")
    val sc: SparkContext = new SparkContext(conf)
    val janeRdd: RDD[(Int, Jane)] = sc.textFile("C:\\Users\\snapp\\Documents\\Courses\\spark-scala\\resources\\JaneAusten.txt")
                            .flatMap(_.split(" "))
                            .map(w => preprocess(w))
                            .map(e => Jane(e))
                            .map(w => (w, 1))
                            .reduceByKey(_ + _)
                            .map(p => (p._2, p._1))
                            .sortByKey(false)

    def preprocess(word: String): String = {
        val pattern = new Regex("[^a-zA-Z0-9\\s]")
        (pattern.replaceAllIn(word, " ")).mkString("")
    }

    def count_words = {
        janeRdd.collect()
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        println(Jane.count_words)
    }
}
