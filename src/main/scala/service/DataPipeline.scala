package service

import org.apache.log4j.Logger

object DataPipeline {

  val log:Logger=Logger.getLogger(getClass)
  def execute(): Unit ={

    //calling spark session from utils
    val sparkSession=utils.ApplicatonUtils.sparkSessionCall()
    //reading the file
    val clickStreamDataRead=FileReader.readFile(sparkSession,"csv",constants.ApplicationConstants.ClickStreamPath)

    val clickStreamPrimariesRemoval = cleanser.FileCleanser.removeRows(clickStreamDataRead,constants.ApplicationConstants.ClickStreamPrimary)

    val clickStreamFillRemaining=cleanser.FileCleanser.fillValues(clickStreamPrimariesRemoval,constants.ApplicationConstants.ClickStreamPrimary,constants.ApplicationConstants.ClickStreamBoolean,constants.ApplicationConstants.clickStreamTimestamp)
    clickStreamFillRemaining.show(10)

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //                item data set
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    val itemDataRead=FileReader.readFile(sparkSession,"csv",constants.ApplicationConstants.ItemDataPath)

    val nullRemovalData = cleanser.FileCleanser.removeRows(itemDataRead,constants.ApplicationConstants.ItemDataPrimary)
    val itemDF = cleanser.FileCleanser.fillValues(nullRemovalData,constants.ApplicationConstants.ItemDataPrimary,constants.ApplicationConstants.ItemDataBoolean,constants.ApplicationConstants.ItemDataTimestamp)
    log.info("Final Data Frame = "+nullRemovalData.count())
    log.info(nullRemovalData)
    itemDF.show(10)

  }
}