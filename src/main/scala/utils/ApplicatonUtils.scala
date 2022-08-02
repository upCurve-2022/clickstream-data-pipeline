package utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ApplicatonUtils {
  def sparkSessionCall(): SparkSession ={
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("DataEngineeringUpCurve")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    sparkSession
  }
}