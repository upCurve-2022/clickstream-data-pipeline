package utils

import exceptions.Exception.NullColException
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ExceptionMethod {
  def validation(df:DataFrame): Boolean ={
    val cols=df.columns.toSeq
    cols.map(colName=>
    {
      if(colName.charAt(0)=='_' && colName.charAt(1)=='c' && colName.charAt(2).isDigit){
        throw NullColException("Null Column Found")
        null
      }
    }
    )
    true
  }
}