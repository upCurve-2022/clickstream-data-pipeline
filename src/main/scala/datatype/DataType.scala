package datatype

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataType {
  def convertStoT(df : DataFrame, col_name: String, format : String) : DataFrame = {
    val new_df = df.withColumn(col_name, to_timestamp(col(col_name),format))
    new_df
  }

  def changeDatatype(df : DataFrame, datatype_seq : Seq[(String,String)]) : DataFrame = {
    val new_df = df.select(datatype_seq.map{case(c,t) => df.col(c).cast(t)}:_*)
    new_df
  }
}