
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException, DuplicateValuesExistException, EmptyFilePathException, FilePathNotFoundException, FileReaderException, FileWriterException, InvalidInputFormatException, NullValuesExistException, SchemaValidationFailedException}
import service.DataPipeline.{execute, log}

import scala.sys.exit


object ClickStream {
  def main(args: Array[String]): Unit = {

    try {
      //performing reader and cleanser operations on both dataset
      execute()

    } catch {
      case ex: Exception => ex.printStackTrace()
        exit(1)
    }
  }
}
