package service

import constants.ApplicationConstants.{CLICK_STREAM_TEST_INPUT_PATH, DATABASE_URL, FILE_FORMAT}
import org.scalatest.flatspec.AnyFlatSpec
import service.FileReader.fileReader

class FileWriterTest extends AnyFlatSpec{

  implicit val spark = utils.ApplicationUtils.createSparkSession(None)

  "file writer " should "write a dataframe to a table " in {
    val inputDF= fileReader(CLICK_STREAM_TEST_INPUT_PATH,FILE_FORMAT )
    val tablename = "unittest"
    service.FileWriter. fileWriter(DATABASE_URL,tablename, inputDF)
  }

}