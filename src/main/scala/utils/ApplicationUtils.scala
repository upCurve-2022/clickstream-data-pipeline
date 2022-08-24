package utils

import com.typesafe.config.{Config, ConfigFactory}
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object ApplicationUtils {

  //configuration
  def configuration(inputPath:String):Config = {
    val parsedConfig = ConfigFactory.parseFile(new File(inputPath))
    val appConf: Config = ConfigFactory.load(parsedConfig)

    appConf
  }


  //Creating the spark session
  def createSparkSession(inputAppConf:Option[Config]): SparkSession = {
    inputAppConf match {
      case Some(x) =>

        implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
          SparkSession.builder
            .appName(x.getString("spark.app.name"))
            .master(x.getString("spark.app.master"))
            .enableHiveSupport()
            .getOrCreate()
        )
spark
      case None =>
        implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
          SparkSession.builder
            .appName("UpCurve Data Pipeline")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark
    }
  }
  //checking for exceptions
  def check(inputDF : DataFrame, colName :String): Unit = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    } else if (!inputDF.columns.contains(colName))
      throw ColumnNotFoundException("The specified column does not exist")
  }
}