package transform

import org.apache.spark.sql.DataFrame
import utils.AppUtils.check

object JoinTransform {
def joinDataFrame(df1:DataFrame,df2:DataFrame,primary_foreignKey:Seq[String]) :DataFrame ={


  primary_foreignKey.foreach { (element: String) => check(df1, element) }

  primary_foreignKey.foreach { (element: String) => check(df2, element) }

  val joinedDataFrame:DataFrame=df1.join(df2,primary_foreignKey,"inner")
  joinedDataFrame
}
}
