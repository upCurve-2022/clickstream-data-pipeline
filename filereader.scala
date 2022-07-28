package reader
import appConstants.Constants
import java.io.FileNotFoundException
import java.io.IOException
import org.apache.spark.sql.{DataFrame, SparkSession}
object filereader {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    try {
      def Reader(format: String, path: String): DataFrame = {
        var fileDF: DataFrame = sparkSession.read.option("header", "true").format(format).load(path)
        fileDF
      }
      // code to access the function should be put here
      val clickstreamDF: DataFrame = Reader("csv",Constants.CLICKSTREAM_PATH)
      val itemDF: DataFrame = Reader("csv",Constants.ITEM_DATA_PATH)
      itemDF.show()

    }
    catch {
      case ex: FileNotFoundException => {
        println("File not found Exception")
      }
      case ex: IOException => {
        println("Input / Output Exception")
      }
    }
  }
}
