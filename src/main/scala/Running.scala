package SparkScala

object Main {
    def main(args: Array[String]): Unit = {
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
            
        JaneAusten.countUnique
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/jane_austin_unique_words")

        Clustering.computeKMeans("C1")
                .coalesce(1)
                .write
                .csv(f"results/kmeans_cost_C1")
        
        Clustering.computeKMeans("C2")
                .coalesce(1)
                .write
                .csv(f"results/kmeans_cost_C2")
        
        Clustering.computeKMeans("C3")
                .coalesce(1)
                .write
                .csv(f"results/kmeans_cost_C3")

        SparkClient.spark.close()
    }
}