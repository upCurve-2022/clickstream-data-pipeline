package exceptions

import org.apache.spark.sql.DataFrame

object Exception {

  def validation(df:DataFrame): Boolean ={
    val cols=df.columns.toSeq
    cols.map(colName=>
      {
        if(colName.charAt(0)=='_' && colName.charAt(1)=='c' && colName.charAt(2).isDigit){
          throw new Exception("Null Column Found")
          null
        }
      }
    )
    true
  }
}