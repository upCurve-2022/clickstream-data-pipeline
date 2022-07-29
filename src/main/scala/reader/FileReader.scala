package reader

import org.apache.spark.sql.DataFrame
import utils.SessionSpark.spark

object FileReader {
  def fileReader(filePath:String, fileType:String): DataFrame = {
    val outputDF = spark.read.option("header", "true").format(fileType).load(filePath)
    outputDF
  }

}
