package cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, desc, lower, row_number, to_timestamp}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer

object FileCleanser {

  /**************MODIFYING COLUMN DATA TYPES*********************/
  //converts string to timestamp format
  def stringToTimestamp(inputDF : DataFrame, colName: String, inputFormat : String) : DataFrame = {
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName),inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF : DataFrame, colName: String) : DataFrame = {
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF : DataFrame, colDatatype : List[(String, String)]) : DataFrame = {
    val outputDF = inputDF.select(colDatatype.map{case(c,t) => inputDF.col(c).cast(t)}:_*)
    outputDF
  }

  /******************REMOVING DUPLICATES FROM THE DATASET******************/
  //function to remove duplicates
  def removeDuplicates(inputDF: DataFrame, primaryKeyCols : Seq[String], orderByCol: String) : DataFrame = {
    val outputDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col(_)):_*).orderBy(desc(orderByCol))))
      .filter(col("rn") === 1).drop("rn")
    outputDF

  }

  /*********************REMOVING NULLS FROM THE DATASET****************************/
  //removing null rows when columns matches not null keys
  def nullRemoval(inputDF: DataFrame, notNullKeys:Seq[String]): DataFrame = {
    val outputDF = inputDF.na.drop("any",notNullKeys)
    outputDF
  }

  //
  def isBooleanPresent(inputDF:DataFrame):List[String]={
    var list = new ListBuffer[String]()
    inputDF.schema.fields.foreach(f=>if(f.dataType.equals(BooleanType)) {
      list=list:+f.name}
    )
    val finals=list.toList
    finals
  }

  //
  def handleTimeStamp(inputDF:DataFrame): DataFrame ={
    val newDf1 = inputDF.withColumn("event_timestamp", coalesce(col("event_timestamp"), current_timestamp()))
    val outputDF = newDf1.withColumn("event_timestamp", to_timestamp(col("event_timestamp"),"MM/dd/yyyy HH:mm"))
    outputDF
  }

  //
  def handleBoolean(inputDF:DataFrame, list:List[String]): DataFrame={
    fillBoolean(inputDF,list.isEmpty,list)
  }

  //
  def fillBoolean(inputDF:DataFrame, bool: Boolean, list: List[String]) = bool match {
    case false => inputDF.na.fill(false,list); case true => inputDF
  }

  //
  def fillUnknown(inputDF:DataFrame, notNullKeys:Seq[String]): DataFrame ={
    val col = inputDF.schema.fieldNames.toSeq
    val colSet=col.toSet
    val notNullSet=notNullKeys.toSet
    val flagColSet=isBooleanPresent(inputDF).toSet
    val nonPrimaries= (colSet.diff(notNullSet)).diff(flagColSet).toSeq
    val filledUnknownDataFrame:DataFrame = inputDF.na.fill("unknown",nonPrimaries)
    filledUnknownDataFrame
  }


}
