package transform

import constants.ApplicationConstants.{DATE_FORMAT, EVENT_DATE, RECORD_LOAD_TIME, TIME_STAMP_COL}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, to_date}
import utils.ApplicationUtils.check

object JoinDatasets {
  def joinDataFrame(df1:DataFrame,df2:DataFrame,joinKey:Seq[String],joinType:String) :DataFrame ={


    joinKey.foreach { (element: String) => check(df1, element) }
    joinKey.foreach { (element: String) => check(df2, element) }

    val joinedDataFrame:DataFrame=df1.join(df2,joinKey,joinType)
    joinedDataFrame
  }



  def transformDataFrame(df:DataFrame) :DataFrame={

    val newDfJoin=   df.withColumn(EVENT_DATE,to_date(df.col(TIME_STAMP_COL),DATE_FORMAT))
    val nextJoin=newDfJoin.withColumn(RECORD_LOAD_TIME,current_timestamp())

    return nextJoin
  }
}
