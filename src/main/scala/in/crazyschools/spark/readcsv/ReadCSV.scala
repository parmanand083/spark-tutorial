package in.crazyschools.spark.readcsv

import org.apache.spark.sql.{DataFrame, SparkSession}
object ReadCSV {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val rawData:DataFrame=sparkSession.read.option("header","true")
      .option("delimiter","~").csv("Data/weblog.csv")
    rawData.printSchema()
    rawData.show(2)
  }
}
