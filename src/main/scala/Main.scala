import FileCleanser.{ColumnModifier, StringToTimestamp}
import AppConstants.Constants
import Filereader.FileReader
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("MyPractice").master("local[*]").getOrCreate()
    //File Reader
    val click_stream_csv_df: DataFrame = FileReader.fileReader(sparkSession, "csv", Constants.CLICK_STREAM_PATH)
    val item_stream_csv_df: DataFrame = FileReader.fileReader(sparkSession, "csv", Constants.ITEM_DATA_PATH)


    // FileCleanser
    //stringToTimestamp
    val new_Df: DataFrame = StringToTimestamp(click_stream_csv_df, "event_timestamp")


    //redirection_sourceToLowerCase
    val ne_df = new_Df.withColumn("redirection_source", lower(col("redirection_source")))

    val n_df = FileCleanser.ToLowerCase(ne_df, "redirection_source")
    //colDataTypeModifier
    val df_click: DataFrame = ColumnModifier(Constants.colDataType_click_stream, n_df)
    val new_item_df: DataFrame = ColumnModifier(Constants.colDataType_item_data, item_stream_csv_df);


    //nullRowRemove
    //    50171
    val new_click_df: DataFrame = FileCleanser.NullRowRemoval(df_click)

//  show
    new_click_df.show()
    new_item_df.show()
  }
}