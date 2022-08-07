package cleanser

import org.apache.spark.sql._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileCleanser {
  //removing rows when primary key is null
  def removeRows(df: DataFrame, primaryColumns:Seq[String]): DataFrame = {
    val rowEliminatedDf = df.na.drop("any",primaryColumns)
    rowEliminatedDf
  }
  def fillCustomValues(df:DataFrame,cols:Seq[String],fillingValue:String):DataFrame={
    val df1=df.na.fill(fillingValue,cols)
    df1
  }
  def fillCurrentTime(df:DataFrame,cols:Seq[String]): DataFrame={
    val currentTime = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm").format(LocalDateTime.now)
    val fillTimeDF=df.na.fill(currentTime,cols);
    fillTimeDF
  }
}