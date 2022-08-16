package transform

import cleanser.FileCleanser.stringToTimestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object TransformOperations {
  def addDate(df:DataFrame): DataFrame ={
    val newDF=df.withColumn("event_d",to_date(col("event_timestamp"),"yyyy-MM-dd HH:mm:ss"))
    newDF
  }

  def addTimeStamp(currDF:DataFrame): DataFrame ={
    val df=currDF.withColumn("record_load_ts",current_timestamp().as("record_load_ts"))
    val formatedDF=stringToTimestamp(df,"record_load_ts",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    formatedDF
  }
}