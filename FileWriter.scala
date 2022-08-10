package service

import exceptions.Exceptions.{DataframeIsEmptyException, FileWriterException}
import org.apache.spark.sql.DataFrame

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
  def writeToOutputPath(inputDF: DataFrame, filePath: String, fileFormat: String): Unit = {

    if (inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    try {
      inputDF.repartition(1).write.format(fileFormat).option("header", "true").mode("overwrite").save(filePath)
    } catch {
      case e: Exception => throw FileWriterException("Error in writing file to given location")
    }
  }
    def fileWriter(tablename: String, df: DataFrame): Unit = {
      val DBURL = "jdbc:mysql://localhost:3306/target_project" //change in conf
      try {
        df.write.format("jdbc")
          .option("url", DBURL)
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", tablename)
          .option("user", "root") //set in conf
          .option("password", "thejasree") // set in conf
          .mode("overwrite")
          .save()
      }catch{
        case e: Exception => e.printStackTrace()
      }
    }
}
