package cleanser

import constants.ApplicationConstants
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import utils.ApplicationUtils.checkExceptions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, desc, exp, lower, row_number, to_timestamp}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer

object FileCleanser {

  /**************MODIFYING COLUMN DATA TYPES*********************/
  //converts string to timestamp format
  def stringToTimestamp(inputDF : DataFrame, colName: String, inputFormat : String) : DataFrame = {
    checkExceptions(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName),inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF : DataFrame, colName: String) : DataFrame = {
    checkExceptions(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF : DataFrame, colDatatype : List[(String, String)]) : DataFrame = {
    val colList = colDatatype.map(x => x._1)
    colList.foreach { (element: String) => checkExceptions(inputDF, element) }
    //val dataTypeList = colDatatype.map(x => x._2)
    val outputDF = inputDF.select(colDatatype.map{case(c,t) => inputDF.col(c).cast(t)}:_*)
    outputDF
  }

  /******************REMOVING DUPLICATES FROM THE DATASET******************/
  //function to remove duplicates
  def removeDuplicates(inputDF: DataFrame, primaryKeyCols : Seq[String], orderByCol: String) : DataFrame = {
    primaryKeyCols.toList.foreach{ (element: String) => checkExceptions(inputDF, element) }
    val outputDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col):_*).orderBy(desc(orderByCol))))
      .filter(col("rn") === 1).drop("rn")
    outputDF

  }

  /*********************REMOVING NULLS FROM THE DATASET****************************/
  //removing null rows when columns matches not null keys
  def nullRemoval(inputDF: DataFrame, notNullKeys:Seq[String]): DataFrame = {
    notNullKeys.toList.foreach{ (element: String) => checkExceptions(inputDF, element) }
    val outputDF = inputDF.na.drop("any",notNullKeys)
    outputDF
  }

  //
  def isBooleanPresent(inputDF:DataFrame):List[String]={
    if(inputDF.count() == 0){
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
    if(inputDF.count() == 0){
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    val newDf1 = inputDF.withColumn(ApplicationConstants.TIME_STAMP_COL, coalesce(col(ApplicationConstants.TIME_STAMP_COL), current_timestamp()))
    val outputDF = newDf1.withColumn(ApplicationConstants.TIME_STAMP_COL, to_timestamp(col(ApplicationConstants.TIME_STAMP_COL),ApplicationConstants.INPUT_TIME_STAMP_FORMAT))
    outputDF
  }

  //
  def handleBoolean(inputDF:DataFrame, list:List[String]): DataFrame={
    list.toList.foreach{ (element: String) => checkExceptions(inputDF, element) }
    fillBoolean(inputDF,list.isEmpty,list)
  }

  //
  def fillBoolean(inputDF:DataFrame, bool: Boolean, list: List[String]): DataFrame = {
    list.foreach{ (element: String) => checkExceptions(inputDF, element) }
    if (bool) {
      inputDF
    } else {
      inputDF.na.fill(value = false, list)
    }
  }

  //
  def fillUnknown(inputDF:DataFrame, notNullKeys:Seq[String]): DataFrame ={
    notNullKeys.toList.foreach{ (element: String) => checkExceptions(inputDF, element) }
    val col = inputDF.schema.fieldNames.toSeq
    val colSet=col.toSet
    val notNullSet=notNullKeys.toSet
    val flagColSet=isBooleanPresent(inputDF).toSet
    val nonPrimaries= colSet.diff(notNullSet).diff(flagColSet).toSeq
    val filledUnknownDataFrame:DataFrame = inputDF.na.fill("unknown",nonPrimaries)
    filledUnknownDataFrame
  }

}
