package service

import org.apache.log4j.Logger
object DataPipeline {

  val log:Logger=Logger.getLogger(getClass)
  def execute(): Unit ={

    //calling spark session from utils
    val sparkSession=utils.ApplicatonUtils.sparkSessionCall()
    //read the file
    val clickStreamDataRead=FileReader.readFile(sparkSession,constants.ApplicationConstants.FILE_FORMAT,constants.ApplicationConstants.CLICK_STREAM_INPUT_PATH)

    //change the data types
    val timeStampDataTypeDF=datatype.DataType.stringToTimestamp(clickStreamDataRead,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF=datatype.DataType.colDatatypeModifier(timeStampDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_DATATYPE_LIST)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedClickStreamDF = cleanser.FileCleanser.removeRows(changeDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    // fill time stamp
    val timeFilledDF=cleanser.FileCleanser.fillCurrentTime(rowEliminatedClickStreamDF,Seq("event_timestamp"))
    timeFilledDF.show(5)
    // fill null values
    val nullFilledClickSteamDF=cleanser.FileCleanser.fillValues(timeFilledDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    nullFilledClickSteamDF.show(10)


//--------------------------------------------------------------------------------------
    //read the data frame
    val itemDataRead=FileReader.readFile(sparkSession,constants.ApplicationConstants.FILE_FORMAT,constants.ApplicationConstants.ITEM_DATA_INPUT_PATH)

    //change the data type
    val dataTypesDF=datatype.DataType.colDatatypeModifier(itemDataRead,constants.ApplicationConstants.ITEM_DATATYPE_LIST)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedItemDF = cleanser.FileCleanser.removeRows(dataTypesDF,constants.ApplicationConstants.ITEM_DATA_NOT_NULL_KEYS)

    //fill null values
    val nullFilledItemF=cleanser.FileCleanser.fillValues(rowEliminatedItemDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)
    nullFilledItemF.show(10)

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //                Join
    /////////////////////////////////////////////////////////////////////////////////////////////////////


    //join the columns
    val joinedLeftDataFrame = transform.JoinDatasets.joinDataFrame(nullFilledClickSteamDF,nullFilledItemF,"left")
    joinedLeftDataFrame.filter("department_name is NULL").show(20)

    //fill null values
    val joinedTransform=cleanser.FileCleanser.fillValues(joinedLeftDataFrame,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)



    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //                Transform
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    //add a new column event_d
    //extract date from TimeStamp Column
    val addDateDF=transform.TransformOperations.addDate(joinedTransform)
    addDateDF.show(10)

    //add Timestamp when the record is loaded into the table
    val addTimeStampDF=transform.TransformOperations.addTimeStamp(addDateDF)
    addTimeStampDF.show(10)

  }
}