import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import datatype.DataType
import appConstants.Constants
import org.apache.spark.sql._
import cleanser.FileCleanser
import reader.FileReader
import org.apache.log4j.Logger
object DataPipeline {

  val log=Logger.getLogger(getClass)
  def main(args: Array[String]) {

    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("DataEngineeringUpCurve")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    /*Data Cleansing : Null Removal */
    val df=FileReader.readFile(sparkSession,"csv",appConstants.Constants.ClickStreamPath)
    FileCleanser.nullRemoval(df,appConstants.Constants.ClickStreamPrimary)

    val itemData=FileReader.readFile(sparkSession,"csv",appConstants.Constants.ClickStreamPath)
    FileCleanser.nullRemoval(itemData,appConstants.Constants.ClickStreamPrimary)

    /*Exception Handling*/
    val clickStreamReadData=FileReader.readFile(sparkSession,"csv",appConstants.Constants.ClickStreamPath)


    val clickStreamGetDataType = DataType.convertStoT(clickStreamReadData, "event_timestamp", "MM/dd/yyyy HH:mm")
    val clickStreamChangeDataType = DataType.changeDatatype(clickStreamGetDataType, Constants.DataTypeClickStream)


    //Checking for flag columns
    val flagColList=cleanser.FileCleanser.isBooleanPresent(clickStreamChangeDataType)
    val flagFilledData=cleanser.FileCleanser.handleBoolean(clickStreamChangeDataType,flagColList)

    //deal with the timeStamp Explicitly
    val timeStampData = cleanser.FileCleanser.handleTimeStamp(flagFilledData)

    //remove rows as per the primaries
    val nullRemovalDataFrame  = FileCleanser.nullRemoval(timeStampData,appConstants.Constants.ClickStreamPrimary)

    //overwrite the remaining col with unknown
    val nullRemovalWithUnknownFilledValues = cleanser.FileCleanser.fillUnknown(nullRemovalDataFrame,appConstants.Constants.ClickStreamPrimary)

    log.info("Final Data Frame = "+nullRemovalWithUnknownFilledValues.count())
    log.info(nullRemovalWithUnknownFilledValues)

  }
}