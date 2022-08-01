package exceptions

object Exceptions{
  case class DataframeIsEmptyException(message:String) extends Exception(message)
  case class ColumnNotFoundException(message:String) extends Exception(message)
}
