package utils

import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ApplicationUtils {
  //Creating the spark session
  def createSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
    SparkSession.builder
    .appName("DataPipeline")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
    )
    spark
  }

  //checking for exceptions
  def checkExceptions(inputDF : DataFrame, colName :String): Unit = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    else if(!inputDF.columns.contains(colName))
      throw ColumnNotFoundException("The specified column does not exist")
  }


}
