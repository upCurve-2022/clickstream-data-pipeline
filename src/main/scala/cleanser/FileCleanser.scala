package cleanser

import constants.AppConstants
import exceptions.Exceptions.{ColumnNotFoundException,DataframeIsEmptyException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, desc, lower, row_number, to_timestamp}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer

object FileCleanser {

  /**************MODIFYING COLUMN DATA TYPES*********************/
  //converts string to timestamp format
  def stringToTimestamp(inputDF : DataFrame, colName: String, inputFormat : String) : DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!inputDF.columns.contains(colName)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName),inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF : DataFrame, colName: String) : DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!inputDF.columns.contains(colName)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF : DataFrame, colDatatype : List[(String, String)]) : DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    val colList = colDatatype.map(x => x._1)
    if(!checkColumn(inputDF, colList)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    //val dataTypeList = colDatatype.map(x => x._2)
    val outputDF = inputDF.select(colDatatype.map{case(c,t) => inputDF.col(c).cast(t)}:_*)
    outputDF
  }

  /******************REMOVING DUPLICATES FROM THE DATASET******************/
  //function to remove duplicates
  def removeDuplicates(inputDF: DataFrame, primaryKeyCols : Seq[String], orderByCol: String) : DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!checkColumn(inputDF, primaryKeyCols.toList) || !inputDF.columns.contains(orderByCol)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    val outputDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col):_*).orderBy(desc(orderByCol))))
      .filter(col("rn") === 1).drop("rn")
    outputDF

  }

  /*********************REMOVING NULLS FROM THE DATASET****************************/
  //removing null rows when columns matches not null keys
  def nullRemoval(inputDF: DataFrame, notNullKeys:Seq[String]): DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!checkColumn(inputDF, notNullKeys.toList)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    val outputDF = inputDF.na.drop("any",notNullKeys)
    outputDF
  }

  //
  def isBooleanPresent(inputDF:DataFrame):List[String]={
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    var list = new ListBuffer[String]()
    inputDF.schema.fields.foreach(f=>if(f.dataType.equals(BooleanType)) {
      list=list:+f.name}
    )
    val finals=list.toList
    finals
  }

  //
  def handleTimeStamp(inputDF:DataFrame): DataFrame ={
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    val newDf1 = inputDF.withColumn(AppConstants.TIME_STAMP_COL, coalesce(col(AppConstants.TIME_STAMP_COL), current_timestamp()))
    val outputDF = newDf1.withColumn(AppConstants.TIME_STAMP_COL, to_timestamp(col(AppConstants.TIME_STAMP_COL),AppConstants.INPUT_TIME_STAMP_FORMAT))
    outputDF
  }

  //
  def handleBoolean(inputDF:DataFrame, list:List[String]): DataFrame={
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!checkColumn(inputDF, list)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    fillBoolean(inputDF,list.isEmpty,list)
  }

  //
  def fillBoolean(inputDF:DataFrame, bool: Boolean, list: List[String]): DataFrame = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!checkColumn(inputDF, list)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    if (bool) {
      inputDF
    } else {
      inputDF.na.fill(value = false, list)
    }
  }

  //
  def fillUnknown(inputDF:DataFrame, notNullKeys:Seq[String]): DataFrame ={
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    if(!checkColumn(inputDF, notNullKeys.toList)){
      throw ColumnNotFoundException("The specified column does not exist")
    }
    val col = inputDF.schema.fieldNames.toSeq
    val colSet=col.toSet
    val notNullSet=notNullKeys.toSet
    val flagColSet=isBooleanPresent(inputDF).toSet
    val nonPrimaries= colSet.diff(notNullSet).diff(flagColSet).toSeq
    val filledUnknownDataFrame:DataFrame = inputDF.na.fill("unknown",nonPrimaries)
    filledUnknownDataFrame
  }

  //
  def checkColumn(inputDF:DataFrame, columns: List[String]) : Boolean ={
    val check = columns.map(x => inputDF.columns.contains(x))
    if(check.contains(false)){
      return false
    }
    true
  }


}
