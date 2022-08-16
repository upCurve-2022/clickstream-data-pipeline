package transform

import org.apache.spark.sql.DataFrame

object JoinDatasets {
  def joinDataFrame(df1:DataFrame, df2:DataFrame, joinType:String): DataFrame ={

    val df1ForeignKey=constants.ApplicationConstants.CLICK_STREAM_FOREIGN_KEY
    val joinExtra=df1.join(df2,df1ForeignKey,joinType)
    joinExtra
  }
}
