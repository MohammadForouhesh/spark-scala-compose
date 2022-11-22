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

        Clustering.computeKMeans(1)
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/kmeans_cost_C1")
        
        Clustering.computeKMeans(2)
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/kmeans_cost_C2")
        
        Clustering.computeKMeans(3)
                .coalesce(1)
                .write.format("com.databricks.spark.csv")
                .option("header", "true")
                .csv(f"results/kmeans_cost_C3")

        SparkClient.spark.close()
    }
}