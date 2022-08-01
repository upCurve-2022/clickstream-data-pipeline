package service

import exceptions.Exceptions.DataframeIsEmptyException
import org.apache.spark.sql.{AnalysisException, DataFrame}
import utils.ApplicationUtils.createSparkSession

object FileReader {
  def fileReader(filePath: String, fileType: String): DataFrame = {
    try {
      val outputDF = createSparkSession().read.option("header", "true").format(fileType).load(filePath)
      if(outputDF.count() == 0){
        throw DataframeIsEmptyException("The dataframe created does not contains any data")
      }
      outputDF
    } catch {
      //case ex: AnalysisException =>
        //throw new AnalysisException(s"The file path $filePath is not found")
      case ex1 : DataframeIsEmptyException =>
        throw DataframeIsEmptyException(ex1.message)
      case _: Exception =>
        throw new Exception("An unknown exception occurred")
    }
  }

}
