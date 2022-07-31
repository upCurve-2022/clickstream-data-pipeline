package writer

import exceptions.Exceptions.DataframeIsEmptyException
import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
  def fileWriter (inputDF : DataFrame, filePath : String, fileFormat: String) : Try[Unit] = {
    try {
      if (inputDF.count() == 0){
        throw DataframeIsEmptyException("The dataframe is empty")
      }
      inputDF.repartition(1).write.option("header", "true").mode("overwrite").format(fileFormat).save(filePath)
      Success(filePath)
    }
    catch {
      case ex: java.lang.ClassNotFoundException =>
        println(s"The file format $fileFormat is not valid")
        Failure(ex)
      case ex1 : DataframeIsEmptyException =>
        println(ex1.message)
        Failure(ex1)
      case unknown: Exception=>
        println(s"Unknown exception : $unknown")
        Failure(unknown)
    }
  }
}
