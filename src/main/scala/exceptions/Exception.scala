package exceptions

object Exception {
  case class NullColException(message:String) extends Exception(message)
}