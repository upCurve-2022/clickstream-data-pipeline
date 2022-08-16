package datatype

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataType {

  def stringToTimestamp(inputDF : DataFrame, colName: String, inputFormat : String) : DataFrame = {
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName),inputFormat))
    outputDF
  }
  def colDatatypeModifier(inputDF : DataFrame, colDatatype : List[(String, String)]) : DataFrame = {

    val outputDF = inputDF.select(colDatatype.map{case(c,t) => inputDF.col(c).cast(t)}:_*)
    outputDF
  }

}