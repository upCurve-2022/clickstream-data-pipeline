package service

import constants.ApplicationConstants.{WRITE_FORMAT, DB_USER, ENCRYPTED_DATABASE_PASSWORD, JDBC_DRIVER}
import exceptions.Exceptions.DatabaseException
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.util.Base64

object FileWriter {

  def decryptPassword(encryptedPasswordPath: String): String = {
    //reads from file and decrypt password
    val source = scala.io.Source.fromFile(encryptedPasswordPath)
    val encryptedPassword = try source.mkString finally source.close()
    val decoded = Base64.getDecoder.decode(encryptedPassword)

    //returns the decrypted password as a string
    val password = new String(decoded, StandardCharsets.UTF_8)
    password
  }


  def fileWriter(databaseURL: String, tableName: String, inputDF: DataFrame): Unit = {
    val password = decryptPassword(ENCRYPTED_DATABASE_PASSWORD)
    //writes dataframe into database
    try {inputDF.write.format(WRITE_FORMAT)
        .option("url", databaseURL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", tableName)
        .option("user", DB_USER)
        .option("password", password)
        .mode("overwrite")
        .save()
    }
    catch {
      case _: Exception => throw DatabaseException("Database connection is not established")
    }
  }
}
