package cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import utils.ApplicationUtils.commonChecks

object FileCleanser {

  /** *********************** null handling *********************** **/
  //removes rows when primary key is null
  def removeRows(inputDF: DataFrame, primaryColumns: Seq[String]): DataFrame = {
    commonChecks(inputDF, primaryColumns)
    val rowEliminatedDF = inputDF.na.drop("any", primaryColumns)
    rowEliminatedDF
  }

  //fills null value with a custom value
  def fillValues(inputDF: DataFrame, nullMap: Map[String, Any]): DataFrame = {
    val colList = nullMap.keys.toSeq
    commonChecks(inputDF, colList)
    val filledDf: DataFrame = inputDF.na.fill(nullMap)
    filledDf
  }

  /** *********************** datatype conversion *********************** **/
  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF: DataFrame, colDatatype: List[(String, String)]): DataFrame = {
    val colList = colDatatype.map(x => x._1)
    commonChecks(inputDF, colList)
    val outputDF = inputDF.select(colDatatype.map { x => inputDF.col(x._1).cast(x._2) }: _*)
    outputDF
  }

  //converts string to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    commonChecks(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }

  /** *********************** data regularisation *********************** **/
  //converts the string to lowercase
  def toLowercase(inputDF: DataFrame, colName: String): DataFrame = {
    commonChecks(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  /** *********************** duplicate removal *********************** **/
  def removeDuplicates(inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: Option[String] = None): DataFrame = {
    commonChecks(inputDF, primaryKeyCols)
    orderByCol match {
      case Some(column) =>
        //Remove duplicates from the click stream dataset
        commonChecks(inputDF, Seq(column))
        val dfRemoveDuplicates = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") === 1).drop("rn")
        dfRemoveDuplicates

      case None =>
        //Remove duplicates from the item dataset
        val dfRemoveDuplicates = inputDF.dropDuplicates(primaryKeyCols)
        dfRemoveDuplicates
    }
  }
}