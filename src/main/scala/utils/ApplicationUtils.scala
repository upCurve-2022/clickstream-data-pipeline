package utils

import com.typesafe.config.{Config, ConfigFactory}
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object ApplicationUtils {

  //configuration
  val parsedConfig = ConfigFactory.parseFile(new File("C:\\Users\\DELL\\Desktop\\sparkAssignment\\stageproject\\clickstream-data-pipeline\\config\\local.conf"))
  val appConf: Config = ConfigFactory.load(parsedConfig)
  val appName = appConf.getString("spark.app.name")
  val appMaster = appConf.getString("spark.app.master")

  //creating spark session
  implicit val sparkSession: SparkSession = SparkSession.builder().appName(appName).master(appMaster).getOrCreate()





  //checking for exceptions

      throw DataframeIsEmptyException("The dataframe is empty")
    }
    else if (!inputDF.columns.contains(colName))
      throw ColumnNotFoundException("The specified column does not exist")
  }






  //Creating the spark session
  //  def createSparkSession(): SparkSession = {
  //    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
  //    SparkSession.builder
  //    .appName(appName)
  //    .master(appMaster)
  //    .enableHiveSupport()
  //    .getOrCreate()
  //    )
  //    spark
  //  }




}
