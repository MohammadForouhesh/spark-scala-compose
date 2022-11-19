name := "word count"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.3.1",
    "org.apache.spark" %% "spark-mllib" % "3.3.1"
)