package in.crazyschools.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
object RDD {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
     val rawData:RDD[String]=sparkSession.sparkContext.textFile("Data/log.txt")
     val lines:Array[String]=rawData.take(5)
     print(lines.mkString("\n"))
  }


}
