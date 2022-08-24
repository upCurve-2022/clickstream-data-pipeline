package service

import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.io.Source

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
//  def writeToOutputPath(inputDF: DataFrame, filePath: String, fileFormat: String): Unit = {
//
//    if (inputDF.count() == 0) {
//      throw DataframeIsEmptyException("The dataframe is empty")
//    }
//    try {
//      inputDF.repartition(1).write.format(fileFormat).option("header", "true").mode("overwrite").save(filePath)
//    } catch {
//      case e: Exception => throw FileWriterException("Error in writing file to given location")
//    }
//  }



  def decryptPassword(encryptedPasswordPath:String):String={
    //read from file and decrypt password
    val decryptedPassword = Source.fromFile(encryptedPasswordPath).getLines.mkString
    val decoded = Base64.getDecoder.decode(decryptedPassword)

    //return the decrypted password as a string
    val password = new String(decoded, StandardCharsets.UTF_8)
    password
  }

  def fileWriter(databaseURL:String,tableName: String, df: DataFrame): Unit = {
    //change in conf

    val password = decryptPassword(constants.ApplicationConstants.ENCRYPTED_DATABASE_PASSWORD)
    try {
        df.write.format("jdbc")
          .option("url", databaseURL)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("dbtable", tableName)
          .option("user", "root")
          .option("password",password)
          .mode("overwrite")
          .save()
    }catch{
      case e: Exception => e.printStackTrace()
    }
  }
}
