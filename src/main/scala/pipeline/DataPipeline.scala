package pipeline

import cleanser.FileCleanser.{colDatatypeModifier, fillUnknown, handleBoolean, handleTimeStamp, isBooleanPresent, nullRemoval, removeDuplicates, stringToTimestamp, toLowercase}
import constants.AppConstants
import org.apache.log4j.Logger
import reader.FileReader.fileReader
import writer.FileWriter.fileWriter

object DataPipeline {
  val log = Logger.getLogger(getClass)

  /** **********CLICK STREAM DATASET************** */

  def clickStreamDataModification(): Unit = {
    //reading click stream dataset
    val clickStreamDF = fileReader(AppConstants.CLICK_STREAM_PATH, "csv")

    //removing nulls from the click stream dataset
    val clickStreamNotNullDF = nullRemoval(clickStreamDF,AppConstants.CLICK_STREAM_NOT_NULL_KEYS)

    //converting string to timestamp format
    val convertedDF = stringToTimestamp(clickStreamNotNullDF, "event_timestamp", "MM/dd/yyyy HH:mm")

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(convertedDF, "redirection_source")

    //modifying the data types of the columns of the click stream dataset
    val modifiedClickStreamDF = colDatatypeModifier(modifiedDF, AppConstants.CLICK_STREAM_DATATYPE)

    //Checking for flag columns
    val flagColList=isBooleanPresent(modifiedClickStreamDF)
    val flagFilledData=handleBoolean(modifiedClickStreamDF,flagColList)

    //deal with the timeStamp Explicitly
    val timeStampData = handleTimeStamp(flagFilledData)

    //remove rows as per the primaries
    val nullRemovalDataFrame  = nullRemoval(timeStampData,AppConstants.CLICK_STREAM_NOT_NULL_KEYS)

    //overwrite the remaining col with unknown
    val nullRemovalWithUnknownFilledValues = fillUnknown(nullRemovalDataFrame,AppConstants.CLICK_STREAM_NOT_NULL_KEYS)

    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(nullRemovalWithUnknownFilledValues, AppConstants.CLICK_STREAM_PRIMARY_KEYS, "event_timestamp")

    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

    //writing the resultant data to a file
    fileWriter(clickStreamDFWithoutDuplicates, AppConstants.CLICK_STREAM_OUTPUT_PATH, "csv")
  }

  /** **************ITEM DATASET*************** */

  def itemDataModification(): Unit = {
    //reading item dataset
    val itemDF = fileReader(AppConstants.ITEM_DATA_PATH, "csv")

    //removing nulls from the item dataset
    val itemNotNullDF = nullRemoval(itemDF,AppConstants.ITEM_NOT_NULL_KEYS)

    //modifying the data types of the columns of the item dataset
    val modifiedItemDF = colDatatypeModifier(itemNotNullDF, AppConstants.ITEM_DATATYPE)

    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(modifiedItemDF, AppConstants.ITEM_PRIMARY_KEYS, "item_id")

    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

    //writing the resultant data of item dataset to a file
    fileWriter(itemDFWithoutDuplicates, AppConstants.ITEM_OUTPUT_PATH, "csv")

  }

}
