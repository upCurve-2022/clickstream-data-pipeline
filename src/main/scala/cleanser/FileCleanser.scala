package cleanser

import org.apache.spark.sql._
import appConstants.Constants
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, to_timestamp}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer
object FileCleanser {

  def nullRemoval(df1: DataFrame, primaries:Seq[String]): DataFrame = {

    //removing null rows when columns matches primaries
    val noNullDF2 = df1.na.drop("any",primaries)
    noNullDF2.show(5)
    noNullDF2
  }

  def isBooleanPresent(df:DataFrame):List[String]={
    var list = new ListBuffer[String]()
    df.schema.fields.foreach(f=>if(f.dataType.equals(BooleanType)) {
      list=list:+f.name}
    )
    val finals=list.toList
    finals
  }
  def handleTimeStamp(df:DataFrame): DataFrame ={
    val newDf1 = df.withColumn("event_timestamp", coalesce(col("event_timestamp"), current_timestamp()))
    val new_df = newDf1.withColumn("event_timestamp", to_timestamp(col("event_timestamp"),"MM/dd/yyyy HH:mm"))
    new_df
  }
  def handleBoolean(df:DataFrame,list:List[String]): DataFrame={
    fillBoolean(df,list.isEmpty,list)
  }
  def fillBoolean(df:DataFrame,b: Boolean,list: List[String]) = b match { case false => df.na.fill(false,list); case true => df }


  def fillUnknown(df:DataFrame,primaries:Seq[String]): DataFrame ={

    val col = df.schema.fieldNames.toSeq
    val colSet=col.toSet
    val primarySet=primaries.toSet
    val flagColSet=isBooleanPresent(df).toSet

    val nonPrimaries= (colSet.diff(primarySet)).diff(flagColSet).toSeq
    val filledUnknownDataFrame:DataFrame = df.na.fill("unknown",nonPrimaries)
    filledUnknownDataFrame
  }

}