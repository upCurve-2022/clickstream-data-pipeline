package service

import checks.DataQualityChecks.{duplicatesCheck, nullCheck, schemaValidationCheck}
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.FileReader.fileReader
import service.FileWriter.fileWriter
import transform.JoinDatasets.{joinDataFrame, transformDataFrame}

object DataPipeline {

  //performs initial steps for click stream dataset
  def clickStreamInitialSteps(database_URL: String, filePath : String, fileFormat : String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //modifying column dataTypes
    val timeStampDataTypeDF = stringToTimestamp(initialDF, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF = colDatatypeModifier(timeStampDataTypeDF, CLICK_STREAM_DATATYPE)

    //eliminate rows on primary keys
    val rowEliminatedDF = removeRows(changeDataTypeDF, CLICK_STREAM_PRIMARY_KEYS)

    // fill null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(nullFilledDF, REDIRECTION_COL)

    //remove duplicates from the dataset
    val DFWithoutDuplicates = removeDuplicates(database_URL, modifiedDF, CLICK_STREAM_PRIMARY_KEYS, Some(TIME_STAMP_COL))

    DFWithoutDuplicates
  }

  //performs initial steps for item dataset
  def itemInitialSteps(database_URL: String, filePath: String, fileFormat: String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //modifying column dataTypes
    val changeDataTypeDF = colDatatypeModifier(initialDF, ITEM_DATATYPE)

    //eliminate rows on primary keys
    val rowEliminatedDF = removeRows(changeDataTypeDF, ITEM_PRIMARY_KEYS)

    //fill null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //remove duplicates from the dataset
    val DFWithoutDuplicates = removeDuplicates(database_URL, nullFilledDF, ITEM_PRIMARY_KEYS, None)

    DFWithoutDuplicates
  }

  //executes the pipeline by joining the two datasets and loading it into mysql database
  def execute(appConf:Config)(implicit sparkSession: SparkSession): Unit = {

    //reading data from conf file
    val clickStreamInputPath: String = appConf.getString(CLICK_STREAM_INPUT_PATH)
    val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
    val database_URL=appConf.getString(DATABASE_URL)
    val schemaPath = appConf.getString(SCHEMA_PATH)

    //initial steps
    val clickStreamDFDeDuplicates = clickStreamInitialSteps(database_URL, clickStreamInputPath, FILE_FORMAT)
    val itemDFDeDuplicates = itemInitialSteps(database_URL, itemDataInputPath, FILE_FORMAT)

    //joining two datasets
    val joinedDataframe = joinDataFrame(clickStreamDFDeDuplicates, itemDFDeDuplicates, JOIN_KEY, JOIN_TYPE)

    //handling null values for joined dataframe
    val nullHandledJoinTable = fillValues(joinedDataframe,COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //transform operation is performed
    val transformJoinedDF = transformDataFrame(nullHandledJoinTable)

    //performing data quality checks on the final dataframe before loading it into mysql database
    val correctSchemaDF = schemaValidationCheck(transformJoinedDF, schemaPath)
    val nullCheckFinalDF:DataFrame = nullCheck(database_URL,correctSchemaDF)
    val duplicateCheckFinalDF = duplicatesCheck(nullCheckFinalDF, FINAL_PRIMARY_KEY, TIME_STAMP_COL)

    //final df to be inserted - write into table
    fileWriter(database_URL, TABLE_NAME, transformJoinedDF)

  }
}
