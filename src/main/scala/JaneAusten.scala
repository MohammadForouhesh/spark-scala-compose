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
        (pattern.replaceAllIn(word, ",")).mkString("").split(",")
    }

    def preprocessStopWord(word: String): String = {
        if (stopWords.contains(word)) "" else word
    }

    def countUnique: DataFrame = {
        countWords(true).filter("count = 1")
    }

    def countWords(removeStopWords: Boolean): DataFrame = {
        val sqlContext: SQLContext = SparkClient.spark.sqlContext
        import sqlContext.implicits._
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
