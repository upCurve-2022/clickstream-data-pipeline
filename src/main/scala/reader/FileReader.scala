package reader

import exceptions.ApplicationException.{DataFrameEmptyException, FileReaderException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReader extends Logging{

  def fileReader( format: String, path: String)(implicit sparkSession: SparkSession): DataFrame = {
    val outputDF = try {
      sparkSession.read.option("header", "true").format(format).load(path)

    } catch {
      case e:Exception=> throw FileReaderException("Unable to read file from given path")

    }
if(outputDF.count()==0) {
throw DataFrameEmptyException("The dataFrame is empty")


}
    outputDF
  }
}
