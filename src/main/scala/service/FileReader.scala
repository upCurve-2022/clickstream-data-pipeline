package service
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.ExceptionMethod.validation

object FileReader {

  def readFile(sparkSession: SparkSession,format:String,path:String): DataFrame ={
    val df=sparkSession.read.option("header",true).format(format).load(path)
    validation(df)
    df
  }
}