package service

import exceptions.Exceptions.DataframeIsEmptyException
import org.apache.spark.sql.DataFrame

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
  def writeToOutputPath(inputDF: DataFrame, filePath: String, fileFormat: String): Unit = {
    try {
      if (inputDF.count() == 0) {
        throw DataframeIsEmptyException("The dataframe is empty")
      }
      inputDF.repartition(1).write.option("header", "true").mode("overwrite").format(fileFormat).save(filePath)
    }
    catch {
      case ex: java.lang.ClassNotFoundException =>
        throw new ClassNotFoundException()
      case ex1: DataframeIsEmptyException =>
        throw DataframeIsEmptyException("The dataframe is empty")
      case unknown: Exception =>
        throw new Exception("An unknown exception occurred")
    }
  }
}
