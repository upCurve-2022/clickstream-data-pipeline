package cleanser

import constants.ApplicationConstants.{ERR_TABLE_DUP_CLICKSTREAM, ERR_TABLE_DUP_ITEM}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import service.FileWriter
import utils.ApplicationUtils.check

object FileCleanser {

  /**************************** REMOVING NULLS FROM THE DATASET ****************************/
  //Handling null values - removing rows when primary key is null
  def removeRows(inputDF: DataFrame, primaryColumns: Seq[String]): DataFrame = {
    check(inputDF, primaryColumns)
    val rowEliminatedDf = inputDF.na.drop("any", primaryColumns)
    rowEliminatedDf
  }

  //Handling null values - filling null value with a custom value
  def fillValues(inputDF: DataFrame, nullMap: Map[String, Any]): DataFrame = {
    val colList = nullMap.keys.toSeq
    check(inputDF, colList)
    val filledDf: DataFrame = inputDF.na.fill(nullMap)
    filledDf
  }

  /********************** MODIFYING COLUMN DATA **********************/
  //converts string to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    check(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF: DataFrame, colName: String): DataFrame = {
    check(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  /********************** MODIFYING COLUMN DATA TYPES **********************/
  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF: DataFrame, colDatatype: List[(String, String)]): DataFrame = {
    val colList = colDatatype.map(x => x._1)
    check(inputDF, colList)
    val outputDF = inputDF.select(colDatatype.map { x => inputDF.col(x._1).cast(x._2) }: _*)
    outputDF
  }

  /******************** REMOVING DUPLICATES FROM THE DATASET ********************/
  //Handling Duplicates
  def removeDuplicates(databaseUrl:String, inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: Option[String]): DataFrame = {
    check(inputDF, primaryKeyCols)
    orderByCol match {
      case Some(column) =>
        //Remove duplicates from the click stream dataset
        val dfRemoveDuplicates = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") === 1).drop("rn")

        //putting duplicate records to the duplicate error table
        val errorDuplicateDF = inputDF.except(dfRemoveDuplicates)
        FileWriter.fileWriter(databaseUrl,ERR_TABLE_DUP_CLICKSTREAM, errorDuplicateDF)

        dfRemoveDuplicates
      //Remove duplicates from the item dataset
      case None =>
        val dfRemoveDuplicates = inputDF.dropDuplicates(primaryKeyCols)

        //putting duplicate records to the duplicate error table
        val errorDuplicateDF = inputDF.except(dfRemoveDuplicates)
        FileWriter.fileWriter(databaseUrl,ERR_TABLE_DUP_ITEM, errorDuplicateDF)

        dfRemoveDuplicates
    }
  }
}