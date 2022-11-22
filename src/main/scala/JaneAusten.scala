package SparkScala

import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.util.matching.Regex
import org.apache.spark.sql.functions.desc
import org.apache.spark.ml.feature.StopWordsRemover


case class Jane(word: String)

object JaneAusten {
    lazy val stopWords: Array[String] = StopWordsRemover.loadDefaultStopWords("english")
    lazy val janeText: DataFrame      = SparkClient.spark.read.text("resources/word-count/JaneAusten.txt")
    lazy val pattern: Regex           = new Regex("[^a-zA-Z]")
    
    def preprocessPunctuation(word: String): Array[String] = {
        pattern.replaceAllIn(word, ",").mkString("").split(",")
    }

    def preprocessStopWord(word: String): String = {
        if (stopWords.contains(word)) "" else word
    }

    def countTotal: DataFrame = {
        countWords(false)
                .agg(Map("word" -> "count", "count" -> "sum"))
                .withColumnRenamed("count(word)", "total_unique_words")
                .withColumnRenamed("sum(count)", "num_total_words")
    }

    def countRareWords: DataFrame = {
        countWords(true).filter("count = 1")
    }

    def countWords(removeStopWords: Boolean): DataFrame = {
        import SparkClient.sqlContext.implicits._

        janeText.flatMap(_.toString.split(" "))
                .flatMap(preprocessPunctuation)
                .map(_.toLowerCase)
                .map(if (removeStopWords) preprocessStopWord else identity)
                .map(_.trim)
                .filter(!_.isEmpty)
                .map(Jane)
                .groupBy("word").count().sort(desc("count"))
    }
}
