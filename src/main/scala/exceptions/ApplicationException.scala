package exceptions

object ApplicationException {


  case class FileReaderException(message: String) extends Exception(message)
  case class DataFrameEmptyException(message: String) extends Exception(message)
  case class ColumnNotFoundException(message: String) extends Exception(message)
  case class FileWriterException(message: String) extends Exception(message)


}

