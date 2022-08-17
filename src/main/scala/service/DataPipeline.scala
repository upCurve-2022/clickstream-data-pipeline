package service

import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants
import constants.ApplicationConstants.{CLICK_STREAM_INPUT_PATH, CLICK_STREAM_OUTPUT_PATH, FILE_FORMAT, ITEM_DATA_INPUT_PATH, ITEM_OUTPUT_PATH}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.FileReader.fileReader
import service.FileWriter.writeToOutputPath
import service.FileWriter.fileWriter
import utils.ApplicationUtils.{configuration, createSparkSession}

object DataPipeline {
  implicit val spark:SparkSession = createSparkSession()
  val appConf: Config = configuration()
  val log: Logger = Logger.getLogger(getClass)
  val clickStreamInputPath: String =appConf.getString(CLICK_STREAM_INPUT_PATH)
  val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
  val clickStreamOutputPath: String=appConf.getString(CLICK_STREAM_OUTPUT_PATH)
  val itemDataOutputPath: String=appConf.getString(ITEM_OUTPUT_PATH)


  def execute(): Unit = {
    //reading click stream dataset

    /*****************CLICK STREAM DATASET**********************/
    val clickStreamDF = fileReader(clickStreamInputPath, FILE_FORMAT)
    //change the data types
    val timeStampDataTypeDF=datatype.DataType.stringToTimestamp(clickStreamDF,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF=datatype.DataType.colDatatypeModifier(timeStampDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_DATATYPE)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedClickStreamDF = cleanser.FileCleanser.removeRows(changeDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    // fill time stamp
    val timeFilledDF=cleanser.FileCleanser.fillCurrentTime(rowEliminatedClickStreamDF,Seq("event_timestamp"))
    // fill null values
    val nullFilledClickSteamDF=cleanser.FileCleanser.fillValues(timeFilledDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    nullFilledClickSteamDF.show(10)

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(nullFilledClickSteamDF, ApplicationConstants.REDIRECTION_COL)

    //modifying the data types of the columns of the click stream dataset
    val modifiedClickStreamDF = colDatatypeModifier(modifiedDF, ApplicationConstants.CLICK_STREAM_DATATYPE)

    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(modifiedClickStreamDF, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, Some(ApplicationConstants.TIME_STAMP_COL))

    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

    //writing the resultant data to a file
    writeToOutputPath(clickStreamDFWithoutDuplicates, clickStreamOutputPath, ApplicationConstants.FILE_FORMAT)

    /** **************ITEM DATASET*************** */
    //reading item dataset
    val itemDF = fileReader(itemDataInputPath, ApplicationConstants.FILE_FORMAT)
    //change the data type
    val dataTypesDF=datatype.DataType.colDatatypeModifier(itemDF,constants.ApplicationConstants.ITEM_DATATYPE)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedItemDF = cleanser.FileCleanser.removeRows(dataTypesDF,constants.ApplicationConstants.ITEM_NOT_NULL_KEYS)

    //fill null values
    val nullFilledItemF=cleanser.FileCleanser.fillValues(rowEliminatedItemDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(nullFilledItemF, ApplicationConstants.ITEM_PRIMARY_KEYS, None)

    //join the columns
    val joinedLeftDataFrame = transform.JoinDatasets.joinDataFrame(nullFilledClickSteamDF,nullFilledItemF,"left")

    //fill null values
    val joinedTransform=cleanser.FileCleanser.fillValues(joinedLeftDataFrame,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //add a new column event_d
    //extract date from TimeStamp Column
    val addDateDF=transform.TransformOperations.addDate(joinedTransform)
    addDateDF.show(10)

    //add Timestamp when the record is loaded into the table
    val addTimeStampDF=transform.TransformOperations.addTimeStamp(addDateDF)

    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

    //writing the resultant data of item dataset to a file
    writeToOutputPath(itemDFWithoutDuplicates, itemDataOutputPath, ApplicationConstants.FILE_FORMAT)


     //final df to be inserted - write into table
    //demo table
    //fileWriter("table2", itemDFWithoutDuplicates)
  }

}
