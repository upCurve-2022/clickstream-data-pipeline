import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.AnalysisException
import service.DataPipeline.{execute, log}
import scala.sys.exit

object ClickStream {
  def main(args: Array[String]): Unit= {

    try{
      //performing reader and cleanser operations on both dataset
      execute()

    }catch {
      case ex : NoClassDefFoundError =>
        log.error("Spark Session is not defined")
        exit(1)
      case ex1 : UnsupportedOperationException =>
        log.error("Conf file is not configured in the correct way")
        exit(1)
      case ex2 : DataframeIsEmptyException =>
        log.error(ex2.message)
        exit(1)
      case ex3 : ColumnNotFoundException =>
        log.error(ex3.message)
        exit(1)
      case ex4 : AnalysisException => {
        log.error(ex4.message)
        exit(1)
      }
      //case ex5 :
    }


  }

}
