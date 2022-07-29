package utils

import org.apache.spark.sql.SparkSession

object SessionSpark {
  //Creating the spark session
  val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder
      .appName("DataPipeline")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
  )

}
