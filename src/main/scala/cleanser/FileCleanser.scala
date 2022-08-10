package cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.AppUtils.check

object FileCleanser {

  //  String to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    check(inputDF, (colName))
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }

  //Converting values of particular column to lower case
  def toLowerCase(inputDF: DataFrame, colName: String): DataFrame = {
    check(inputDF, (colName))
    val outputDF: DataFrame = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  // column datatype modifier
  def colDatatypeModifier(inputDF: DataFrame, colDatatype: Seq[(String, String)]): DataFrame = {
    val newSeq: Seq[String] = colDatatype.map(x => x._1)

    newSeq.foreach { (element: String) => check(inputDF, element) }
    //    CleanserExceptionMethods.check(inputDF, (newSeq))
    val outputDF = inputDF.select(colDatatype.map { case (c, t) => inputDF.col(c).cast(t) }: _*)
    outputDF
  }

  //function to remove duplicates
  def removeDuplicates(inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: String): DataFrame = {
    primaryKeyCols.foreach { (element: String) => check(inputDF, element) }
    check(inputDF, orderByCol)
    val dfRemoveDuplicates = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col(_)): _*).orderBy(desc(orderByCol))))
      .filter(col("rn") === 1).drop("rn")
    dfRemoveDuplicates
  }


}

