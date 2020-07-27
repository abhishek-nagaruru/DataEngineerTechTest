package com.csvreadwrite.main
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import scala.io.Source

object App {

  def executeAnalysis(inPath: String, outPath: String, credentailsFile: String) {
    var accessKey = ""
    var secret = ""
    val src = Source.fromFile(credentailsFile).getLines().foreach(line => {
      val cvals = line.split(",").map(_.trim())
      accessKey = cvals(0)
      secret = cvals(1)
    })
    val spark = SparkSession.builder().appName("csv reader").master("local").getOrCreate();
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    val values = spark.read.csv(inPath)
    val rawdf = values.toDF("one", "two")
    val valuesToKeyMap = rawdf.groupBy("one", "two").count().sort("one", "two").toDF("one", "two", "three")
    val filterOdd = valuesToKeyMap.filter(valuesToKeyMap("one").notEqual("0")).filter(valuesToKeyMap("three").%(2).!==(0)).toDF()
    val filterByOnes = filterOdd.groupBy("one").count().toDF("one", "count")
    val filterOnlyOnes = filterByOnes.filter(filterByOnes("count").equalTo("1")).toDF()
    val resultantDF = filterOdd.join(filterOnlyOnes, Seq("one"), "right").toDF()
    val result = resultantDF.select("one", "three").toDF("one", "two")
    result.write.format("csv").save(outPath)
    result.show()
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val inPath = args(0)
    val outPath = args(1)
    val credentialsFile = args(2)
    executeAnalysis(inPath, outPath, credentialsFile)
  }
}