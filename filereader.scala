package sparkcodes.practice
import java.io.FileNotFoundException
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
    try{
    def Reader(format: String, path: String): DataFrame = {
      var fileDF: DataFrame = sparkSession.read.option("header", "true").format(format).load(path)
      fileDF;
    }
    //testing
      // code to access the function should be put here
      val file: DataFrame = Reader("csv", "data/item/item.csv")
      file.show()
    }
    catch {
      case ex: FileNotFoundException => {
        println("File not found Exception")
      }
    }
  }
}
