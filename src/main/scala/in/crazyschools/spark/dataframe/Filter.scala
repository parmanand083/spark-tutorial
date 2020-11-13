package in.crazyschools.spark.dataframe
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Filter {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val rawData:DataFrame=sparkSession.read.option("header","true")
      .option("delimiter",",").csv("Data/salaries.csv")

    // Filer condition
    val newDF1:DataFrame=rawData.filter(" TotalPay > 500 and TotalPay < 1000")
    val newDF11:DataFrame=rawData.where(" TotalPay > 500 and TotalPay < 1000")
    newDF11.show(2)


    // Filer condition
    val newDF2:DataFrame=rawData.filter(col("TotalPay")<500 and col("TotalPay") >0)
    val newDF22:DataFrame=rawData.where(col("TotalPay")<500 and col("TotalPay") >0)
    newDF22.show(2)

  }
}
