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
    
    //handling null values
    val rowEliminatedDF = removeRows(clickStreamDF, ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    val valueFilledDF = fillValues(rowEliminatedDF, ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS, ApplicationConstants.CLICK_STREAM_BOOLEAN, ApplicationConstants.CLICK_STREAM_TIMESTAMP)
    
    //converting string to timestamp format
    val convertedDF = stringToTimestamp(valueFilledDF, ApplicationConstants.TIME_STAMP_COL, ApplicationConstants.INPUT_TIME_STAMP_FORMAT)

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(convertedDF, ApplicationConstants.REDIRECTION_COL)

    //modifying the data types of the columns of the click stream dataset
    val modifiedClickStreamDF = colDatatypeModifier(modifiedDF, ApplicationConstants.CLICK_STREAM_DATATYPE)
    
    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(nullRemovalWithUnknownFilledValues, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, ApplicationConstants.TIME_STAMP_COL)

    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

    //writing the resultant data to a file
    writeToOutputPath(clickStreamDFWithoutDuplicates, ApplicationConstants.CLICK_STREAM_OUTPUT_PATH, ApplicationConstants.FILE_FORMAT)

    /** **************ITEM DATASET*************** */

    //reading item dataset
    val itemDF = fileReader(ApplicationConstants.ITEM_DATA_PATH, ApplicationConstants.FILE_FORMAT)
    
    //handling null values for item dataset
    val rowEliminatedItemDF = removeRows(itemDF, ApplicationConstants.ITEM_NOT_NULL_KEYS)
    val valueFilledItemDF = fillValues(rowEliminatedItemDF, ApplicationConstants.ITEM_NOT_NULL_KEYS, ApplicationConstants.ITEM_DATA_BOOLEAN, ApplicationConstants.ITEM_DATA_TIMESTAMP)

    //modifying the data types of the columns of the item dataset
    val modifiedItemDF = colDatatypeModifier(valueFilledItemDF, ApplicationConstants.ITEM_DATATYPE)

    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(itemNotNullDF, ApplicationConstants.ITEM_PRIMARY_KEYS, "item_id")

    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

    //writing the resultant data of item dataset to a file
    writeToOutputPath(itemDFWithoutDuplicates, ApplicationConstants.ITEM_OUTPUT_PATH, ApplicationConstants.FILE_FORMAT)
    
  }

}
