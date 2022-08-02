package cleanser

import org.apache.log4j.Logger
import org.apache.spark.sql._
import constants.ApplicationConstants
import datatype.DataType
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, to_timestamp}
import org.apache.spark.sql.types.{BooleanType, TimestampType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
object FileCleanser {
  /*def nullRemovalClickStreamData(clickStreamReadData:DataFrame,log: Logger): DataFrame = {
    Try{
      val value= exceptions.Exception.validation(clickStreamReadData)
    } match {
      case Success(value) =>

        val clickStreamGetDataType = DataType.convertStoT(clickStreamReadData, "event_timestamp", "MM/dd/yyyy HH:mm")
        val clickStreamChangeDataType = DataType.changeDatatype(clickStreamGetDataType, ApplicationConstants.DataTypeClickStream)

        //Checking for flag columns
        val flagColList=cleanser.FileCleanser.booleanColumn(clickStreamChangeDataType)
        val flagFilledData=cleanser.FileCleanser.handleBoolean(clickStreamChangeDataType,flagColList)

        //deal with the timeStamp Explicitly
        val isTime=cleanser.FileCleanser.isTimestampPresent(flagFilledData)
        val timeStampData = cleanser.FileCleanser.handleTimeStamp(flagFilledData,isTime)

        //remove rows as per the primaries
        val nullRemovalDataFrame  = FileCleanser.nullRemoval(timeStampData,constants.ApplicationConstants.ClickStreamPrimary)

        //overwrite the remaining col with unknown
        val nullRemovalWithUnknownFilledValues = cleanser.FileCleanser.fillUnknown(nullRemovalDataFrame,constants.ApplicationConstants.ClickStreamPrimary)

        log.info("Final Data Frame = "+nullRemovalWithUnknownFilledValues.count())
        log.info(nullRemovalWithUnknownFilledValues)
        nullRemovalWithUnknownFilledValues

      case Failure(exception) =>
        println(s"Found Exception : $exception")
        null

    }
  }
  def nullRemovalItemData(itemData:DataFrame,log: Logger): DataFrame = {
    Try{
      val value = exceptions.Exception.validation(itemData)
    } match {
      case Success(value)=>

        val nullRemovalData = cleanser.FileCleanser.nullRemoval(itemData,constants.ApplicationConstants.ItemDataPrimary)
        val itemDF = cleanser.FileCleanser.fillUnknown(nullRemovalData,constants.ApplicationConstants.ItemDataPrimary)
        log.info("Final Data Frame = "+nullRemovalData.count())
        log.info(nullRemovalData)
        itemDF

      case Failure(exception)=>
        println(s"Found Exception : exception")
        null

    }
  }*/
  //1.removing rows when primary key is null
  def removeRows(df: DataFrame, primaryColumns:Seq[String]): DataFrame = {
    val rowEliminatedDf = df.na.drop("any",primaryColumns)
    rowEliminatedDf
  }

  //2.filling null values
  def fillValues(df:DataFrame, primaryColumns:Seq[String], booleanColumns:Seq[String], timestampColumns:Seq[String]):DataFrame = {
    //filling false
    val booleanFilledDf:DataFrame = df.na.fill("FALSE":String,booleanColumns)
    //filling current timestamp
    val currentTime = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm").format(LocalDateTime.now)
    val timestampFilledDf:DataFrame = booleanFilledDf.na.fill(currentTime:String,timestampColumns)

    //filling unknown
    val remainingColumns = (df.columns.toSet).diff(((primaryColumns.toSet).union(booleanColumns.toSet)).union(timestampColumns.toSet)).toSeq
    val unknownFilledDf:DataFrame = timestampFilledDf.na.fill("unknown":String,remainingColumns)
    unknownFilledDf
  }
}