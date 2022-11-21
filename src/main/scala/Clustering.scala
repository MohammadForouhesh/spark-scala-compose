package SparkScala

import org.apache.spark.mllib.clustering.{KMeans, BisectingKMeans}
import org.apache.spark.sql.{SQLContext, Column}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.monotonically_increasing_id

object Clustering {
    lazy val numIterations: Int = 100
    import SparkClient.sqlContext.implicits._
    def computeKMeans(path: String) = {
        val data = SparkClient.sc.textFile(f"resources/clustering/$path.txt")
                             .map(_.trim)
                             .map(_.split("    "))
                             .map(s => Vectors.dense(s.map(_.toDouble)))
                             .cache()

        (2 to 25).map(KMeans.train(data, _, numIterations).computeCost(data))
                .toDF()
                .withColumn("numClusters", monotonically_increasing_id + 2)
    }
}
