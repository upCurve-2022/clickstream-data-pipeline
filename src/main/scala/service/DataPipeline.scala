package service

import cleanser.FileCleanser._
import constants.ApplicationConstants
import org.apache.log4j.Logger
import service.FileReader.fileReader
import service.FileWriter.writeToOutputPath

object DataPipeline {
  val log: Logger = Logger.getLogger(getClass)

  /** **********CLICK STREAM DATASET************** */

  def execute(): Unit = {
    //reading click stream dataset
    val clickStreamDF = fileReader(ApplicationConstants.CLICK_STREAM_PATH, ApplicationConstants.FILE_FORMAT)
    println(clickStreamDF.show())
    println("******1******")

    //converting string to timestamp format
    val convertedDF = stringToTimestamp(clickStreamDF, ApplicationConstants.TIME_STAMP_COL, ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    println("******2******")
    //converting redirection column into lowercase
    val modifiedDF = toLowercase(convertedDF, ApplicationConstants.REDIRECTION_COL)
    println("******3******")
    //modifying the data types of the columns of the click stream dataset
    val modifiedClickStreamDF = colDatatypeModifier(modifiedDF, ApplicationConstants.CLICK_STREAM_DATATYPE)
    println("******4******")
    //Checking for flag columns
    val flagColList = isBooleanPresent(modifiedClickStreamDF)
    println("******5******")
    val flagFilledData = handleBoolean(modifiedClickStreamDF, flagColList)
    println("******6******")
    //deal with the timeStamp Explicitly
    val timeStampData = handleTimeStamp(flagFilledData)
    println("******7******")
    //remove rows as per the primaries
    val nullRemovalDataFrame = nullRemoval(timeStampData, ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    println("******8******")
    //overwrite the remaining col with unknown
    val nullRemovalWithUnknownFilledValues = fillUnknown(nullRemovalDataFrame, ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    println("******9******")
    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(nullRemovalWithUnknownFilledValues, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, ApplicationConstants.TIME_STAMP_COL)
    println("******10******")
    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())
    println("******11******")
    //writing the resultant data to a file
    writeToOutputPath(clickStreamDFWithoutDuplicates, ApplicationConstants.CLICK_STREAM_OUTPUT_PATH, ApplicationConstants.FILE_FORMAT)
    println("******12******")
    /** **************ITEM DATASET*************** */

    //reading item dataset
    val itemDF = fileReader(ApplicationConstants.ITEM_DATA_PATH, ApplicationConstants.FILE_FORMAT)
    println("******13******")
    //modifying the data types of the columns of the item dataset
    val modifiedItemDF = colDatatypeModifier(itemDF, ApplicationConstants.ITEM_DATATYPE)
    println("******14******")
    //removing nulls from the item dataset
    val itemNotNullDF = nullRemoval(modifiedItemDF, ApplicationConstants.ITEM_NOT_NULL_KEYS)
    println("******15******")
    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(itemNotNullDF, ApplicationConstants.ITEM_PRIMARY_KEYS, "item_id")
    println("******16******")
    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())
    println("******17******")
    //writing the resultant data of item dataset to a file
    writeToOutputPath(itemDFWithoutDuplicates, ApplicationConstants.ITEM_OUTPUT_PATH, ApplicationConstants.FILE_FORMAT)
    println("******18*****")
  }

}
