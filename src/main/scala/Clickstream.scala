
import org.apache.log4j.Logger

import scala.util.Try

object Clickstream {

  val log:Logger=Logger.getLogger(getClass)
  def main(args: Array[String]) {
    try{
      service.DataPipeline.execute()
    }
    catch
    {
      case exception: Exception=>println("Exception found : "+exception)
    }
  }
}