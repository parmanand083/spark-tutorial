package in.crazyschools.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
object CacheAndPersist {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("TestAPP").master("local[2]").getOrCreate()
    val rawData:DataFrame=sparkSession.read.option("header","true")
      .option("delimiter",",").csv("Data/weblog.csv")

    val rawCachedDF:DataFrame=rawData.cache()
     val count:Long= rawCachedDF.count()


     // if you want to use persist
    val rawPersistDF:DataFrame=rawData.persist(StorageLevel.MEMORY_ONLY)
    val rowCount:Long= rawCachedDF.count()


  }

}
