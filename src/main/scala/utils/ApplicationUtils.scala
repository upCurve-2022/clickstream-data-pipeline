package utils

import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants.{APP_MASTER, APP_NAME}
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.io.Source

object ApplicationUtils {

  //creates the configuration
  def createConfiguration(inputPath: String): Config = {
    val parsedConfig = ConfigFactory.parseFile(new File(inputPath))
    val resolvedConfig: Config = ConfigFactory.load(parsedConfig)
    resolvedConfig
  }

  //creates the spark session
  def createSparkSession(inputAppConf: Config): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName(inputAppConf.getString(APP_NAME))
        .master(inputAppConf.getString(APP_MASTER))
        .getOrCreate()
    )
    spark
  }

  //checks for exceptions
  def commonChecks(inputDF: DataFrame, colName: Seq[String]): Unit = {
    if (inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    } else {
      colName.foreach { (element: String) =>
        if (!inputDF.columns.contains(element))
          throw ColumnNotFoundException("The specified column does not exist")
      }
    }
  }

  //extracts the schema from the json file
  def schemaRead(schemaPath : String) : StructType = {
    val source = Source.fromFile(schemaPath)
    val schemaJson = try source.getLines().mkString finally source.close()
    val correctSchema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
    correctSchema
  }
}