package service

import checks.DataQualityChecks.{duplicatesCheck, nullCheck, schemaValidationCheck}
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants
import constants.ApplicationConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import service.FileReader.fileReader
import service.FileWriter.{encryptPassword, writeToOutputPath}
import transform.JoinDatasets.joinDataFrame
import utils.ApplicationUtils.{configuration, createSparkSession}

object DataPipeline {
  implicit val spark: SparkSession = createSparkSession()
  val appConf: Config = configuration()
  val log: Logger = Logger.getLogger(getClass)
  val clickStreamInputPath: String = appConf.getString(CLICK_STREAM_INPUT_PATH)
  val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
  val clickStreamOutputPath: String = appConf.getString(CLICK_STREAM_OUTPUT_PATH)
  val itemDataOutputPath: String = appConf.getString(ITEM_OUTPUT_PATH)


  def execute(): Unit = {

    /*****************CLICK STREAM DATASET**********************/
    //reading click stream dataset
    val clickStreamDF = fileReader(clickStreamInputPath, FILE_FORMAT)

    //change the data types
    val timeStampDataTypeDF=cleanser.FileCleanser.stringToTimestamp(clickStreamDF,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF=cleanser.FileCleanser.colDatatypeModifier(timeStampDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_DATATYPE)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedClickStreamDF = cleanser.FileCleanser.removeRows(changeDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    // fill time stamp
    val timeFilledDF=cleanser.FileCleanser.fillCurrentTime(rowEliminatedClickStreamDF,constants.ApplicationConstants.CLICK_STREAM_TIMESTAMP)
    // fill null values
    val nullFilledClickSteamDF=cleanser.FileCleanser.fillValues(timeFilledDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)


    //converting redirection column into lowercase
    val modifiedDF = toLowercase(nullFilledClickSteamDF, ApplicationConstants.REDIRECTION_COL)

    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(modifiedDF, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, Some(ApplicationConstants.TIME_STAMP_COL))

    //performing data quality checks on click stream dataset
    val clickStreamMandatoryCol = CLICK_STREAM_DATATYPE.map(x => x._1)
    val itemMandatoryCol = ITEM_DATATYPE.map(x => x._1)
    nullCheck(clickStreamDFWithoutDuplicates, clickStreamMandatoryCol)
    schemaValidationCheck(clickStreamDFWithoutDuplicates)
    duplicatesCheck(clickStreamDFWithoutDuplicates, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, Some(ApplicationConstants.TIME_STAMP_COL))

    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

    //writing the resultant data to a file
    //writeToOutputPath(clickStreamDFWithoutDuplicates, clickStreamOutputPath, ApplicationConstants.FILE_FORMAT)

    /** **************ITEM DATASET*************** */
    //reading item dataset
    val itemDF = fileReader(itemDataInputPath, ApplicationConstants.FILE_FORMAT)

    //change the data type
    val dataTypesDF=cleanser.FileCleanser.colDatatypeModifier(itemDF,constants.ApplicationConstants.ITEM_DATATYPE)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedItemDF = cleanser.FileCleanser.removeRows(dataTypesDF,constants.ApplicationConstants.ITEM_NOT_NULL_KEYS)

    //fill null values
    val nullFilledItemF=cleanser.FileCleanser.fillValues(rowEliminatedItemDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(nullFilledItemF, ApplicationConstants.ITEM_PRIMARY_KEYS, None)

    //performing data quality checks on item dataset
    nullCheck(itemDFWithoutDuplicates, itemMandatoryCol)
    schemaValidationCheck(itemDFWithoutDuplicates)
    duplicatesCheck(itemDFWithoutDuplicates, ApplicationConstants.ITEM_PRIMARY_KEYS, None)

    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

    //  joining two datasets
    val joinedDataframe = joinDataFrame(clickStreamDFWithoutDuplicates, itemDFWithoutDuplicates, join_key, join_type)

    val nullHandledJoinTable=fillValues(joinedDataframe,itemDataNullFillValues)

    // transform
    val transformJoinedDF = transform.JoinDatasets.transformDataFrame(nullHandledJoinTable)
    transformJoinedDF.show()

    //testing
    val df = transformJoinedDF.filter(transformJoinedDF.col("department_name") === "unknown" && transformJoinedDF.col("product_type") === "unknown" && transformJoinedDF.col("vendor_id") === (-1) &&
      transformJoinedDF.col("item_price") === (-1))
    transformJoinedDF.printSchema()

    //writing the resultant data of item dataset to a file
    //writeToOutputPath(itemDFWithoutDuplicates, itemDataOutputPath, ApplicationConstants.FILE_FORMAT)

    //final df to be inserted - write into table
    //demo table
    //run to encrypt password, only needed to be done once.
    encryptPassword()
    //fileWriter("table2", itemDFWithoutDuplicates)

  }
}
