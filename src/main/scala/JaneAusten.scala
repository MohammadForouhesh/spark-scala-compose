package word_count

import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.util.matching.Regex
import org.apache.spark.sql.functions.desc
import org.apache.spark.ml.feature.StopWordsRemover


case class Jane(word: String)

object JaneAusten {
    lazy val stopWords: Array[String] = StopWordsRemover.loadDefaultStopWords("english")
    lazy val janeText: DataFrame      = SparkClient.spark.read.text("resources/JaneAusten.txt")
    lazy val pattern: Regex           = new Regex("[^a-zA-Z0-9]")
    
    def notPunctuation(word: String): Array[String] = {
        (pattern.replaceAllIn(word, ",")).mkString("").split(",")
    }

    def notStopWord(word: String): String = {
        if (stopWords.contains(word)) "" else word
    }

    def count_words: DataFrame = {
        val sqlContext: SQLContext = SparkClient.spark.sqlContext
        import sqlContext.implicits._
        janeText.flatMap(_.toString.split(" "))
                .flatMap(notPunctuation)
                .map(_.toLowerCase)
                .map(notStopWord)
                .map(_.trim)
                .filter(!_.isEmpty)
                .map(Jane)
                .groupBy("word").count().sort(desc("count"))
    }
}

object Main {
    def main(args: Array[String]): Unit = {
        JaneAusten.count_words.show()
        SparkClient.spark.close()
    }
}