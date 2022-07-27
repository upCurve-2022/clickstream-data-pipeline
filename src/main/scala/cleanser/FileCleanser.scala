package cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, to_timestamp}

object FileCleanser {

  //  String to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }


  //Converting values of particular column to lower case
  def toLowerCase(inputDF: DataFrame, colName: String): DataFrame = {
    val outputDF: DataFrame = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  // column datatype modifier
  def colDatatypeModifier(inputDF: DataFrame, colDatatype: Seq[(String, String)]): DataFrame = {
    val outputDF = inputDF.select(colDatatype.map { case (c, t) => inputDF.col(c).cast(t) }: _*)
    outputDF
  }

}
