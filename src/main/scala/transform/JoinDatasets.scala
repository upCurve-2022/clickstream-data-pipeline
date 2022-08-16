package transform

<<<<<<< HEAD
import constants.ApplicationConstants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, to_date}
import utils.ApplicationUtils.check

object JoinDatasets {
  def joinDataFrame(df1: DataFrame, df2: DataFrame, joinKey: Seq[String], joinType: String): DataFrame = {


    joinKey.foreach { (element: String) => check(df1, element) }
    joinKey.foreach { (element: String) => check(df2, element) }

    val joinedDataFrame: DataFrame = df1.join(df2, joinKey, joinType)
joinedDataFrame
//    val joinedTableNullFill = fillCustomValues(joinedDataFrame,itemDataNullFillValues)
//    joinedTableNullFill
  }


  def transformDataFrame(df: DataFrame): DataFrame = {

    val newDfJoin = df.withColumn(EVENT_DATE, to_date(df.col(TIME_STAMP_COL), DATE_FORMAT))
    val nextJoin = newDfJoin.withColumn(RECORD_LOAD_TIME, current_timestamp())

    nextJoin
  }
=======
object JoinDatasets {

>>>>>>> 3193cfa3108ef36f3dfafd3c133b1b47ed3e2eb8
}
