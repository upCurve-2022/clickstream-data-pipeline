package reader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FileReader {

  def readFile(sparkSession: SparkSession,format:String,path:String): DataFrame ={
    val df=sparkSession.read.option("header",true).format(format).load(path)
    df
  }
}