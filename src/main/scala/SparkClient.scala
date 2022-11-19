package word_count

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.log4j.{Logger, Level}


object SparkClient {
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
