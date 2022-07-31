package reader
import utils.SessionSpark.spark
import org.apache.spark.sql.{AnalysisException, DataFrame}

import scala.util.{Failure, Success, Try}

object FileReader {
  def fileReader(filePath:String, fileType:String): Try[DataFrame] = {
    try{
      val outputDF = spark.read.option("header", "true").format(fileType).load(filePath)
      Success(outputDF)
    }catch {
      case ex : AnalysisException =>
        println(s"The file $filePath does not exist")
        Failure(ex)
      case unknown : Exception =>
        println(s"Unknown exception : $unknown")
        Failure(unknown)
    }
  }

}
