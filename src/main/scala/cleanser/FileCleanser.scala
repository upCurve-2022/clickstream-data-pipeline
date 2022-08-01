package cleanser

import org.apache.log4j.Logger
import org.apache.spark.sql._
import appConstants.Constants
import datatype.DataType
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, to_timestamp}
import org.apache.spark.sql.types.{BooleanType, TimestampType}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
object FileCleanser {
  def nullRemovalClickStreamData(clickStreamReadData:DataFrame,log: Logger): DataFrame = {
    Try{
      val value= exceptions.Exception.validation(clickStreamReadData)
    } match {
      case Success(value) =>

        val clickStreamGetDataType = DataType.convertStoT(clickStreamReadData, "event_timestamp", "MM/dd/yyyy HH:mm")
        val clickStreamChangeDataType = DataType.changeDatatype(clickStreamGetDataType, Constants.DataTypeClickStream)

        //Checking for flag columns
        val flagColList=cleanser.FileCleanser.booleanColumn(clickStreamChangeDataType)
        val flagFilledData=cleanser.FileCleanser.handleBoolean(clickStreamChangeDataType,flagColList)

        //deal with the timeStamp Explicitly
        val isTime=cleanser.FileCleanser.isTimestampPresent(flagFilledData)
        val timeStampData = cleanser.FileCleanser.handleTimeStamp(flagFilledData,isTime)

        //remove rows as per the primaries
        val nullRemovalDataFrame  = FileCleanser.nullRemoval(timeStampData,appConstants.Constants.ClickStreamPrimary)

        //overwrite the remaining col with unknown
        val nullRemovalWithUnknownFilledValues = cleanser.FileCleanser.fillUnknown(nullRemovalDataFrame,appConstants.Constants.ClickStreamPrimary)

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

        val nullRemovalData = cleanser.FileCleanser.nullRemoval(itemData,appConstants.Constants.ItemDataPrimary)
        val itemDF = cleanser.FileCleanser.fillUnknown(nullRemovalData,appConstants.Constants.ItemDataPrimary)
        log.info("Final Data Frame = "+nullRemovalData.count())
        log.info(nullRemovalData)
        itemDF

      case Failure(exception)=>
        println(s"Found Exception : exception")
        null

    }
  }
  def nullRemoval(df1: DataFrame, primaries:Seq[String]): DataFrame = {

    //removing null rows when columns matches primaries
    val noNullDF2 = df1.na.drop("any",primaries)
    noNullDF2
  }

  def booleanColumn(df:DataFrame):List[String]={
    var list = new ListBuffer[String]()
    df.schema.fields.foreach(f=>if(f.dataType.equals(BooleanType)) {
      list=list:+f.name}
    )
    val finals=list.toList
    finals
  }

  def isTimestampPresent(df:DataFrame):List[String]={
    var list = new ListBuffer[String]()
    df.schema.fields.foreach(f=>if(f.dataType.equals(TimestampType)) {
      list=list:+f.name}
    )
    val finals=list.toList
    finals
  }

  def handleTimeStamp(df:DataFrame,list:List[String]): DataFrame ={
    var curr=df
    var some=df
    list.foreach(
      str => {
        curr = df.withColumn(str, coalesce(col(str), current_timestamp()))
        some = curr.withColumn("event_timestamp", to_timestamp(col("event_timestamp"),"MM/dd/yyyy HH:mm"))
      }

    )
    val finalDF=some
    finalDF
  }

  def handleBoolean(df:DataFrame,list:List[String]): DataFrame={
      val boolData:DataFrame =   df.na.fill(false,list);
      boolData

  }

  def fillUnknown(df:DataFrame,primaries:Seq[String]): DataFrame ={

    val col = df.schema.fieldNames.toSeq
    val colSet=col.toSet
    val primarySet=primaries.toSet
    val flagColSet=booleanColumn(df).toSet

    val nonPrimaries= (colSet.diff(primarySet)).diff(flagColSet).toSeq

    val filledUnknownDataFrame:DataFrame = df.na.fill("unknown",nonPrimaries)
    filledUnknownDataFrame
  }

}