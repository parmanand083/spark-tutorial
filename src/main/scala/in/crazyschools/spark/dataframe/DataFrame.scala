package in.crazyschools.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrame {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val tempDF:DataFrame=sparkSession.range(1,10,2).toDF("Roll Number")
    tempDF.show()


  }
}
