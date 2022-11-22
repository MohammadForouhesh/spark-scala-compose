package SparkScala

object Main {
    def main(args: Array[String]): Unit = {
        JaneAusten.countTotal
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/jane_austin_word_count_total")

        JaneAusten.countWords(false)
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/jane_austin_word_count_with_stopwords")

        JaneAusten.countWords(true)
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/jane_austin_word_count_without_stopwords")
            
        JaneAusten.countRareWords
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/jane_austin_unique_words")

        for (i <- (1 to 3)) {
            val (opt, pts, cents) = Clustering.optimizeKMeans(i, 2, 25)
            pts.coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/kmeans_cost_C$i")
                
            cents.coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/kmeans_centeriods_{$opt}k_C{$i}")
        }

        for (i <- (1 to 3)) {
            val (opt, pts, cents) = Clustering.optimizeBisectingKMeans(i, 2, 25)
            pts.coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/bisecting_kmeans_cost_C$i")
                
            cents.coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/bisecting_kmeans_centeriods_{$opt}k_C{$i}")
        }
        SparkClient.spark.close()
    }
}