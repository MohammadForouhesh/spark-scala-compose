package word_count

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame}
import scala.util.matching.Regex
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions.desc
import org.apache.spark.ml.feature.StopWordsRemover


case class Jane(word: String)

object Spark {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    private val conf: SparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("word count")
        .set("spark.driver.allowMultipleContexts", "false")

    val spark: SparkSession = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._
}

object JaneAusten {
    lazy val stopWords = StopWordsRemover.loadDefaultStopWords("english")
    
    def notPunctuation(word: String): String = {
        lazy val pattern = new Regex("[^a-zA-Z0-9\\s]")
        (pattern.replaceAllIn(word, " ")).mkString("")
    }

    def notStopWord(word: String): String = {
        if (stopWords.contains(word)) "" else word
    }

    def count_words = {
        val sqlContext: SQLContext = Spark.spark.sqlContext
        import sqlContext.implicits._
        Spark.spark.read.text("resources/JaneAusten.txt")
                .flatMap(_.toString.split(" "))
                .map(w => notPunctuation(w))
                // .map(w => notStopWord(w))
                .map(_.trim)
                .filter(!_.isEmpty())
                .map(w => Jane(w))
                .groupBy("word").count().sort(desc("count"))
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        JaneAusten.count_words.show()
        Spark.spark.close()
    }
}