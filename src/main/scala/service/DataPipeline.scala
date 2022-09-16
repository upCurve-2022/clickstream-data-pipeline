package service

import checks.DataQualityChecks.{duplicatesCheck, nullCheck, schemaValidationCheck}
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants
import constants.ApplicationConstants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.FileReader.fileReader
import service.FileWriter.fileWriter
import transform.JoinDatasets._

object DataPipeline {
  //performs initial steps for click stream dataset
  def clickStreamInitialSteps(filePath: String, fileFormat: String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //modifies column dataTypes
    val timeStampDataTypeDF = stringToTimestamp(initialDF, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF = colDatatypeModifier(timeStampDataTypeDF, CLICK_STREAM_DATATYPE)

    //eliminates rows with null primary key
    val rowEliminatedDF = removeRows(changeDataTypeDF, CLICK_STREAM_PRIMARY_KEYS)

    //fills custom null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    //converts redirection column into lowercase
    val modifiedDF = toLowercase(nullFilledDF, REDIRECTION_COL)

    //removes duplicates
    val DFDeDuplicates = removeDuplicates(modifiedDF, CLICK_STREAM_PRIMARY_KEYS, Some(TIME_STAMP_COL))

    DFDeDuplicates
  }

  //performs initial steps for item dataset
  def itemInitialSteps(filePath: String, fileFormat: String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //modifies column dataTypes
    val changeDataTypeDF = colDatatypeModifier(initialDF, ITEM_DATATYPE)

    //eliminates rows with null primary key
    val rowEliminatedDF = removeRows(changeDataTypeDF, ITEM_PRIMARY_KEYS)

    //fills custom null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //removes duplicates
    val DFDeDuplicates = removeDuplicates(nullFilledDF, ITEM_PRIMARY_KEYS, None)

    DFDeDuplicates
  }

  //executes the pipeline by joining the two datasets and loading it into mysql database
  def execute(appConf: Config)(implicit sparkSession: SparkSession): Unit = {

    //inputs data from config
    val clickStreamInputPath: String = appConf.getString(CLICK_STREAM_INPUT_PATH)
    val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
    val database_URL = appConf.getString(DATABASE_URL)
    val schemaPath = appConf.getString(SCHEMA_PATH)

    //returns click stream dataframe without duplicates
    val clickStreamDFDeDuplicates = clickStreamInitialSteps(clickStreamInputPath, READ_FORMAT)

    //returns item dataframe without duplicates
    val itemDFDeDuplicates = itemInitialSteps(itemDataInputPath, READ_FORMAT)

    //joins two datasets
    val joinedDataframe = joinDataFrame(clickStreamDFDeDuplicates, itemDFDeDuplicates, JOIN_KEY, JOIN_TYPE)

    //handles null values for joined dataframe
    val nullHandledJoinTable = fillValues(joinedDataframe, COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //transforms joined dataframe
    val transformJoinedDF = transformDataFrame(nullHandledJoinTable)

    //performs data quality checks
    val correctSchemaDF = schemaValidationCheck(transformJoinedDF, schemaPath)
    val nullCheckFinalDF: DataFrame = nullCheck(database_URL, correctSchemaDF)
    val duplicateCheckFinalDF = duplicatesCheck(database_URL, nullCheckFinalDF, FINAL_PRIMARY_KEY, TIME_STAMP_COL)

    //prints final dataframe
    duplicateCheckFinalDF.printSchema()
    duplicateCheckFinalDF.show(20)

    //writes final dataframe into MySQL database
    fileWriter(database_URL, TABLE_NAME, duplicateCheckFinalDF)
  }
}
