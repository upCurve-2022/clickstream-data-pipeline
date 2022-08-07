package service

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object DataPipeline {

  val log:Logger=Logger.getLogger(getClass)
  def execute(): Unit ={


    //calling spark session from utils
    val sparkSession=utils.ApplicatonUtils.sparkSessionCall()
    //read the file
    val clickStreamDataRead=FileReader.readFile(sparkSession,constants.ApplicationConstants.FILE_FORMAT,constants.ApplicationConstants.CLICK_STREAM_INPUT_PATH)
    //call for remove null values rows
    val rowEliminatedClickStreamDF = cleanser.FileCleanser.removeRows(clickStreamDataRead,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)

    val numericFilledDF = cleanser.FileCleanser.fillCustomValues(rowEliminatedClickStreamDF, constants.ApplicationConstants.CLICK_STREAM_NUMERIC, "-1")
    //call for fill current time stamp
    val timestampFilledDf = cleanser.FileCleanser.fillCurrentTime(numericFilledDF,constants.ApplicationConstants.CLICK_STREAM_TIME_STAMP)
    //call for fill boolean
    val falseFilledDF=cleanser.FileCleanser.fillCustomValues(timestampFilledDf,constants.ApplicationConstants.CLICK_STREAM_BOOLEAN,"FALSE")
    //call for fill unknown values
    val unknownFilledClickstreamDf:DataFrame =cleanser.FileCleanser.fillCustomValues(falseFilledDF,constants.ApplicationConstants.CLICK_STREAM_STRING,"UNKNOWN")
    unknownFilledClickstreamDf.show(10)

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //                item data set
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //read the file
    val itemDataRead=FileReader.readFile(sparkSession,"csv",constants.ApplicationConstants.ITEM_DATA_INPUT_PATH)
    //call for remove null values rows
    val rowEliminatedItemDF = cleanser.FileCleanser.removeRows(itemDataRead,constants.ApplicationConstants.ITEM_DATA_NOT_NULL_KEYS)
    //call for fill item_price
    val itemPriceFilledData=cleanser.FileCleanser.fillCustomValues(rowEliminatedItemDF,constants.ApplicationConstants.ITEM_DATA_NUMERIC,"-1")
    //call for fill unknown values
    val unknownFilledItemDf:DataFrame =cleanser.FileCleanser.fillCustomValues(itemPriceFilledData,constants.ApplicationConstants.ITEM_DATA_STRING,"UNKNOWN")
    unknownFilledItemDf.show(10)

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //                join
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    val df1:DataFrame=itemDataRead
    val df2:DataFrame=clickStreamDataRead
    val joinedDataFrame = transform.JoinDatasets.joinDataFrame(df1,df2,"inner").show(10)

  }
}