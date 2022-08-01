package cleanser

import constants.ApplicationConstants
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import utils.ApplicationUtils.checkExceptions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, desc, exp, lower, row_number, to_timestamp}
import org.apache.spark.sql.types.BooleanType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer

object FileCleanser {
  
  /*********************REMOVING NULLS FROM THE DATASET****************************/
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




}
