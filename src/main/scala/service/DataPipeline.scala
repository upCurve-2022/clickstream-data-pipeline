package service

import cleanser.FileCleanser.{colDatatypeModifier, removeDuplicates, stringToTimestamp, toLowerCase}
import constants.Constants
import constants.Constants._
import org.apache.spark.sql.DataFrame
import reader.FileReader.fileReader
import transform.JoinTransform.joinDataFrame
import utils.AppUtils.sparkSession
import writer.FileWriter.fileWriter

object DataPipeline {

  def execute(): Unit = {
    /** ****************************************************CLICK_STREAM AND ITEM  DATASET READING **************************************** */
    //reading clickStream data set
    val clickStreamDF: DataFrame = fileReader(CSV_FORMAT, CLICK_STREAM_INPUT_PATH)
    //reading item dataset
    val itemDF: DataFrame = fileReader(CSV_FORMAT, Constants.ITEM_DATA_INPUT_PATH)
    /** ****************************************************CLICK_STREAM DATASET MODIFYING*********************************** */
    // converting string to timestamp format
    val convertedClickStreamDF: DataFrame = stringToTimestamp(clickStreamDF, EVENT_TIMESTAMP, "MM/dd/yyyy HH:mm")
    //converting redirection source column value into lower case
    val modified1ClickStreamDF = toLowerCase(convertedClickStreamDF, REDIRECTION_SOURCE)
    /** ******************************************CLICK_STREAM AND ITEM  DATASET COLUMN DATATYPE MODIFY********************************************* */
    //modifying the datatype of the columns of clickStream dataset
    val modified2ClickStreamDF: DataFrame = colDatatypeModifier(modified1ClickStreamDF, constants.Constants.CLICK_STREAM_DATATYPE)
    // modifying the datatype of the columns of item dataset
    val modifiedItemDF: DataFrame = colDatatypeModifier(itemDF, constants.Constants.ITEM_DATATYPE);
    /** ************************************CLICK_STREAM AND ITEM  AND JOINED TABLE DATASET WRITING************************************************ */
    //removing duplicate from clickStream
    val deDuplicateClickStreamDF: DataFrame = removeDuplicates(modified2ClickStreamDF, constants.Constants.clickStream_primary_keys, EVENT_TIMESTAMP)
    //    removing duplicates from item dataset
    val deDuplicateItemDF: DataFrame = removeDuplicates(modifiedItemDF, constants.Constants.item_primary_keys, "item_id")
    /** ************************************************************************************************************************* */


    val joinDataframe: DataFrame = joinDataFrame(deDuplicateClickStreamDF, deDuplicateItemDF, item_primary_keys)

    /** ************************************************************************************************************************* */
    //    writing clickStream data
    writer.FileWriter.fileWriter(deDuplicateClickStreamDF, CSV_FORMAT, Constants.CLICK_STREAM_OUTPUT_PATH)

    fileWriter(deDuplicateItemDF, CSV_FORMAT, Constants.ITEM_DATA_OUTPUT_PATH)
    fileWriter(joinDataframe, CSV_FORMAT, Constants.JOIN_TABLE_PATH)
    joinDataframe.show()

    /** ************************************************************************************************************************* */

  }


}
