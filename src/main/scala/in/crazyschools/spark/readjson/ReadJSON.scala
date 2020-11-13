package in.crazyschools.spark.readjson
import org.apache.spark.sql.{DataFrame, SparkSession}
object ReadJSON{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val rawData:DataFrame=sparkSession.read.json("Data/iris.json")
    rawData.printSchema()
    rawData.show(2)
  }
}
