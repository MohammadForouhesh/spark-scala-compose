package SparkScala

import org.apache.spark.mllib.clustering.{KMeans, BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.sql.{SQLContext, DataFrame, Column, Row}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import scala.util.matching.Regex
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.rdd.RDD
import SparkClient.sqlContext.implicits._


case class Centroid(x: Double, y: Double)

object Clustering {
    lazy val numIterations: Int = 100
    lazy val numPattern: Regex  = new Regex("[^0-9.]")
    lazy val infoSeq: Seq[RDD[Vector]] = (1 to 3).map(ind => preprocess(SparkClient.sc.textFile(pathGen(ind))).cache())

    def pathGen(testCaseNum: Int): String = f"resources/clustering/C$testCaseNum.txt"

    def preprocess(resilientData: RDD[String]): RDD[Vector] = {
        resilientData.map(_.trim)
            .map(numericPreprocess)
            .map(w => w.filter(_ != ' '))
            .map(_.split(","))
            .map(s => Vectors.dense(s.map(_.toDouble)))
    }
    
    def numericPreprocess(word: String): String = {
        numPattern.replaceFirstIn(word, ",").mkString("")
    }

    def optimizeKMeans(testCaseNum: Int, start: Int, end: Int) = {
        def gridSearchKMeans: Seq[Double] = 
            (start to end).map(KMeans.train(infoSeq(testCaseNum - 1), _, numIterations).computeCost(infoSeq(testCaseNum - 1)))
        
        def elbowSearchKMeans(gridPoints: Seq[Double]): Int = {
            val slope = (gridPoints.last - gridPoints.head)/(end - start)
            (start to end).map(_ * slope + (gridPoints(0) - (slope * start)))
                            .zip(gridPoints)
                            .map(p => p._1 - p._2)                            
                            .zipWithIndex.maxBy(x => x._1)._2
        }
        
        val pts: Seq[Double] = gridSearchKMeans
        val optimK: Int      = elbowSearchKMeans(pts)
        
        (optimK, 
        pts.toDF().withColumn("numClusters", monotonically_increasing_id + start),
        KMeans.train(infoSeq(testCaseNum - 1), optimK, numIterations)
            .clusterCenters.map(_.toArray)
            .map({case Array(x, y) => Centroid(x, y)})
            .toSeq
            .toDF())
    }
    

    def computeElboBisectingKMeans(testCaseNum: Int): DataFrame = {
        def bkm(k: Int): BisectingKMeansModel = {
            new BisectingKMeans().setK(k).run(infoSeq(testCaseNum))
        }

        (2 to 25).map(bkm(_).computeCost(infoSeq(testCaseNum)))
                .toDF()
                .withColumn("numClusters", monotonically_increasing_id + 2)
    }
}
