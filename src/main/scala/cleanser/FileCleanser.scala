package cleanser

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, map_keys}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileCleanser {
  //removing rows when primary key is null
  def removeRows(df: DataFrame, primaryColumns:Seq[String]): DataFrame = {
    val rowEliminatedDf = df.na.drop("any",primaryColumns)
    rowEliminatedDf
  }
  //filling custom values
  def fillValues(df:DataFrame,colNameDefaultValue:Map[String,Any]): DataFrame ={
    val newDF=df.na.fill(colNameDefaultValue)
    newDF
  }
  //filling current time stamp
  def fillCurrentTime(df:DataFrame,cols:Seq[String]): DataFrame={
    val currentTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)
    val fillTimeDF=df.withColumn("event_timestamp",coalesce(col("event_timestamp"),lit(currentTime)))
//    val currentTime = DateTimeFormatter.ofPattern(constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT).format(LocalDateTime.now)
//    val timestampFilledDf:DataFrame = df.na.fill(currentTime:String,cols)
    fillTimeDF
  }
}