
import com.typesafe.config.Config
import exceptions.Exceptions._
import org.apache.spark.sql.SparkSession
import service.DataPipeline.{execute, log}
import utils.ApplicationUtils.{configuration, createSparkSession}

import scala.sys.exit


object ClickStream {
  def main(args: Array[String]): Unit = {
    val confPath = args(0)
    val appConf: Config = configuration(confPath)

  try {
      //performing reader and cleanser operations on both dataset
      execute(appConf)

    }
    catch {
      case ex: FileReaderException => log.error("File Reader Exception: " + ex.message)
        exit(1)
      case ex: DataframeIsEmptyException => log.error("DataFrameIsEmptyException:" + ex.message)
        exit(1)
      case ex: ColumnNotFoundException => log.error("ColumnNotFoundException:" + ex.message)
        exit(1)
      case ex: EmptyFilePathException => log.error("EmptyFilePathException:" + ex.message)
        exit(1)
      case ex: FilePathNotFoundException => log.error("FilePathNotFoundException:" + ex.message)
        exit(1)
      case ex: InvalidInputFormatException => log.error("InvalidInputFormatException: " + ex.message)
        exit(1)
      case ex: NullValuesExistException => log.error("NullValuesExistException: " + ex.message)
        exit(1)
      case ex: SchemaValidationFailedException => log.error("SchemaValidationFailedException: " + ex.message)
        exit(1)
      case ex: DuplicateValuesExistException => log.error("DuplicateValuesExistException: " + ex.message)
        exit(1)
      case ex: FileWriterException => log.error("FileWriterException:" + ex.message)
        exit(1)
    }
}
    //    } catch {
    //      case ex: Exception => ex.printStackTrace()
    //        exit(1)
    //    }

}
