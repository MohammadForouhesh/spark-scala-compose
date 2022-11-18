package word_count

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex


case class Jane(word: String) extends Serializable 

object JaneAusten extends Serializable {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\bin\\")
    private val conf: SparkConf = new SparkConf()
        .setMaster("spark://127.0.0.1:7077")
        .setAppName("word count")
        .set("spark.driver.allowMultipleContexts", "false")

    val ss: SparkSession = SparkSession
        .builder()
        .config(conf)
        // .config("spark.jars", "target/scala-2.12/word-count_2.12-1.0.jar")
        .getOrCreate()
    val sc: SparkContext = ss.sparkContext
    val sqlContext: SQLContext = ss.sqlContext
    import sqlContext.implicits._

    val janeRdd: RDD[String] = sc.textFile("resources/JaneAusten.txt")

    def preprocess(word: String): String = {
        val pattern = new Regex("[^a-zA-Z0-9\\s]")
        (pattern.replaceAllIn(word, " ")).mkString("")
    }

    def count_words = {
        janeRdd.flatMap(line => line.split(" "))
                .map(w => preprocess(w))
                .map(w => Jane(w))
                .toDF().groupBy("word").count()
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        println(JaneAusten.count_words.show())
    }
}
