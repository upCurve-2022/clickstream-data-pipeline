import exceptions.ApplicationException.{ColumnNotFoundException, DataFrameEmptyException, FileReaderException, FileWriterException}
import org.apache.log4j.Logger

object UpCurvePipe {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      service.DataPipeline.execute()
    } catch {
      case e: FileReaderException =>
        log.error("File reader Exception :" + e.message)
      case e: DataFrameEmptyException =>
        log.error("DataFrame Exception:" + e.message)
      case e: ColumnNotFoundException =>
        log.error("ColumnNotFoundException:" + e.message)
      case e: FileWriterException =>
        log.error("File writer Exception :" + e.message)

    }
  }
}


