package in.crazyschools.spark.dataframe
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object AddDropDFCol {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val rawData:DataFrame=sparkSession.read.option("header","true")
      .option("delimiter",",").csv("Data/salaries.csv")
    rawData.printSchema()
    //adding  new column
    val tempDF:DataFrame=rawData.withColumn("First Name",split(col("EmployeeName")," ")(0))
    tempDF.printSchema()
    tempDF.select("id","EmployeeName","First Name")show(2,false)
    val df:DataFrame=tempDF.withColumnRenamed("First Name","First_Name")
    //dropping notes col
    val finalDF:DataFrame=tempDF.drop("Notes")
    finalDF.show(11,false)
  }
}
