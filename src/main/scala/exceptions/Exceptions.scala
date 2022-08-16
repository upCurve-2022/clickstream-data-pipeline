package exceptions

object Exceptions{
  case class FileReaderException(message: String) extends Exception(message)
  case class FileWriterException(message: String) extends Exception(message)
  case class DataframeIsEmptyException(message:String) extends Exception(message)
  case class ColumnNotFoundException(message:String) extends Exception(message)
  case class EmptyFilePathException(message:String) extends Exception(message)
  case class FilePathNotFoundException(message:String) extends Exception(message)
<<<<<<< HEAD
  case class SchemaValidationFailedException(message : String) extends Exception(message)
  case class NullValuesExistException(message : String) extends Exception(message)
}
=======
}
>>>>>>> 3193cfa3108ef36f3dfafd3c133b1b47ed3e2eb8
