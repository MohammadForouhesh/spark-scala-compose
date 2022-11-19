package word_count

import org.apache.spark.sql.{SQLContext, DataFrame}
import scala.util.matching.Regex
import org.apache.spark.sql.functions.desc
import org.apache.spark.ml.feature.StopWordsRemover


case class Jane(word: String)

object JaneAusten {
    lazy val stopWords = StopWordsRemover.loadDefaultStopWords("english")
    lazy val pattern = new Regex("[^a-zA-Z0-9]")
    
    def notPunctuation(word: String): Array[String] = {
        (pattern.replaceAllIn(word, ",")).mkString("").split(",")
    }

    def notStopWord(word: String): String = {
        if (stopWords.contains(word)) "" else word
    }

    def count_words = {
        val sqlContext: SQLContext = SparkClient.spark.sqlContext
        import sqlContext.implicits._
        SparkClient.spark.read.text("resources/JaneAusten.txt")
                .flatMap(_.toString.split(" "))
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