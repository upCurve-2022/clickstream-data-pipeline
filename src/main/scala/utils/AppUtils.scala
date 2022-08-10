package utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File


object AppUtils {
//  def config
  val parsedConfig = ConfigFactory.parseFile(new File("C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\conf\\local.conf"))
  val appConf: Config = ConfigFactory.load(parsedConfig)
  val appName = appConf.getString("spark.app.name")
  val appMaster = appConf.getString("spark.app.master")
  // val conf=new SparkConf
  // val appName=conf.get("spark.app.name")
  // val masName=conf.get("spark.app.master")
  implicit val sparkSession: SparkSession = SparkSession.builder().appName(appName).master(appMaster).getOrCreate()



  //for checking weather dataframe is null or column does not exist
  def check(inputDF: DataFrame, colName: String): Unit = {
    if (inputDF.count() == 0) {
      throw exceptions.ApplicationException.DataFrameEmptyException(s"The given DataFrame $inputDF is empty")
    } else if (!inputDF.columns.contains(colName)) {
      throw exceptions.ApplicationException.ColumnNotFoundException(s""" Column "$colName" not found in $inputDF dataframe""")
    }
  }
}
