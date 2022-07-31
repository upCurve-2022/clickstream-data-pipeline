package pipeline

import cleanser.FileCleanser.{colDatatypeModifier, fillUnknown, handleBoolean, handleTimeStamp, isBooleanPresent, nullRemoval, removeDuplicates, stringToTimestamp, toLowercase}
import constants.AppConstants
import exceptions.Exceptions.{ColumnNotFoundException, DataTypeNotFoundException, DataframeIsEmptyException}
import org.apache.log4j.Logger
import reader.FileReader.fileReader
import writer.FileWriter.fileWriter
import scala.util.{Failure, Success}

object DataPipeline {
  val log: Logger = Logger.getLogger(getClass)

  /** **********CLICK STREAM DATASET************** */

  def clickStreamDataModification(): Unit = {
    //reading click stream dataset
    fileReader(AppConstants.CLICK_STREAM_PATH, AppConstants.FILE_FORMAT) match {
      case Success(clickStreamDF) =>
        //converting string to timestamp format
        try{
          //val df = spark.emptyDataFrame
          val convertedDF = stringToTimestamp(clickStreamDF, AppConstants.TIME_STAMP_COL, AppConstants.INPUT_TIME_STAMP_FORMAT)

          //converting redirection column into lowercase
          val modifiedDF = toLowercase(convertedDF, AppConstants.REDIRECTION_COL)

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
          val clickStreamDFWithoutDuplicates = removeDuplicates(nullRemovalWithUnknownFilledValues, AppConstants.CLICK_STREAM_PRIMARY_KEYS, AppConstants.TIME_STAMP_COL)

          //logging information about click stream dataset
          log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

          //writing the resultant data to a file
          fileWriter(clickStreamDFWithoutDuplicates, AppConstants.CLICK_STREAM_OUTPUT_PATH, AppConstants.FILE_FORMAT) match {
            case Success(_) =>
              log.info(s"The dataframe is successfully written to ${AppConstants.CLICK_STREAM_OUTPUT_PATH}")
            case Failure(exception) =>
              log.error(s"The dataframe cannot be written to the requested destination because of ${exception.getMessage}")
          }
        }
        catch {
          case ex : DataframeIsEmptyException =>
            log.error(ex.message)
          case ex1 : ColumnNotFoundException =>
            log.error(ex1.message)
          case ex2 : DataTypeNotFoundException =>
            log.error(ex2.message)
        }
      case Failure(exception) =>
        log.error(exception.getMessage)
        log.error("Click Stream Dataframe could not be created.... Terminating the pipeline")
    }
  }

  /** **************ITEM DATASET*************** */

  def itemDataModification(): Unit = {
    //reading item dataset
    fileReader(AppConstants.ITEM_DATA_PATH, AppConstants.FILE_FORMAT) match {
      case Success(itemDF) =>
        //modifying the data types of the columns of the item dataset
        val modifiedItemDF = colDatatypeModifier(itemDF, AppConstants.ITEM_DATATYPE)

        //removing nulls from the item dataset
        val itemNotNullDF = nullRemoval(modifiedItemDF, AppConstants.ITEM_NOT_NULL_KEYS)

        //remove duplicates from the item dataset
        val itemDFWithoutDuplicates = removeDuplicates(itemNotNullDF, AppConstants.ITEM_PRIMARY_KEYS, "item_id")

        //logging information about item dataset
        log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

        //writing the resultant data of item dataset to a file
        fileWriter(itemDFWithoutDuplicates, AppConstants.ITEM_OUTPUT_PATH, AppConstants.FILE_FORMAT) match {
          case Success(_) =>
            log.info(s"The dataframe is successfully written to ${AppConstants.ITEM_OUTPUT_PATH}")
          case Failure(exception) =>
            log.error(s"The dataframe cannot be written to the requested destination because of ${exception.getMessage}")
      }
      case Failure(exception) =>
        log.error(exception.getMessage)
        log.error("Item Dataframe could not be created.... Terminating the pipeline")
    }
  }
}
