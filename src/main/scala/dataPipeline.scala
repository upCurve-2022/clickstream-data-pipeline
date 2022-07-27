import appConstants.Constants
import cleanser.FileCleanser
import org.apache.log4j.Logger
import reader.FileReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object dataPipeline {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    //    creating a spark session
    val sparkSession: SparkSession = SparkSession.builder().appName("Data Pipeline").master("local[*]").getOrCreate()

    /** *****************CLICKSTREAM DATASET***************** */
    //reading clickstream data set
    val clickstreamDF: DataFrame = FileReader.fileReader(sparkSession, "csv", Constants.CLICKSTREAM_PATH)

    // converting string to timestamp format
    val convertedClickstreamDF: DataFrame = FileCleanser.stringToTimestamp(clickstreamDF, "event_timestamp", "MM/dd/yyyy HH:mm")

    //converting redirection source column value into lower case
    val modifierClickstreamDF = FileCleanser.toLowerCase(convertedClickstreamDF, "redirection_source")

    //modifying the datatype of the columns of clickstream datset
    val modifiedClickstreamDF: DataFrame = FileCleanser.colDatatypeModifier(modifierClickstreamDF, appConstants.Constants.CLICKSTREAM_DATATYPE)
    /** *****************ITEM DATASET***************** */
    //reading item dataset
    val itemDF: DataFrame = FileReader.fileReader(sparkSession, "csv", Constants.ITEM_DATA_PATH)


    // modifying the datatype of the columns of item datset
    val modifiedItemDF: DataFrame = FileCleanser.colDatatypeModifier(itemDF, appConstants.Constants.ITEM_DATATYPE);


    //    //nullRowRemove
    //    //    50171
    //    val new_click_df: DataFrame = cleanser.NullRowRemoval(df_click)
    //
    //    //  show


    log.warn(clickstreamDF.rdd.getNumPartitions)
    log.warn("Total record in click dataset :" + clickstreamDF.count().toString)
    log.warn(itemDF.rdd.getNumPartitions)
    log.warn("Total item datset:" + itemDF.count().toString)
val write_df=clickstreamDF.write.format("csv").save("C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\output")
  }
}