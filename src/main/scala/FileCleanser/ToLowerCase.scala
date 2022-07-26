package FileCleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower}

object ToLowerCase {
  def apply(df:DataFrame,colName:String) :DataFrame={
    val n_df :DataFrame= df.withColumn(colName, lower(col(colName)))
    return  n_df
  }
}
