package FileCleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_timestamp}

object StringToTimestamp {
  def apply(csv_df: DataFrame, colName: String): DataFrame = {
    val new_Df = csv_df.withColumn(colName, to_timestamp(col(colName), "MM/dd/yyyy HH:mm"))
    return new_Df
  }


}
