package service

import exceptions.Exceptions.{DataframeIsEmptyException, FileWriterException}
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.io.Source
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

  /*def encrypt(string: String):String={
      val bytes = string.getBytes(StandardCharsets.UTF_8)
      val encoded = Base64.getEncoder.encodeToString(bytes)
      encoded
    }
    val encryptedKey=encrypt("thejasree")//change according to your system password
    val writer = new PrintWriter(new File("data/encrypted_password.txt" ))
    writer.write(encryptedKey)
    writer.close()*/
  val encryptedKey = Source.fromFile("data/encrypted_password.txt").getLines.mkString
  val decoded = Base64.getDecoder.decode(encryptedKey)
  val password = new String(decoded, StandardCharsets.UTF_8)

    def fileWriter(tablename: String, df: DataFrame): Unit = {
      val DBURL = "jdbc:mysql://localhost:3306/target_project" //change in conf
      try {
        df.write.format("jdbc")
          .option("url", DBURL)
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", tablename)
          .option("user", "root")
          .option("password",password) // encrypted password is provided
          .mode("overwrite")
          .save()
      }catch{
        case e: Exception => e.printStackTrace()
      }
    }
}
