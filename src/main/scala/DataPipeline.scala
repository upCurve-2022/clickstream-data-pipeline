import org.apache.spark.sql._
import reader.FileReader
import org.apache.log4j.Logger

object DataPipeline {

  val log:Logger=Logger.getLogger(getClass)
  def main(args: Array[String]) {

    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("DataEngineeringUpCurve")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    /*Data Cleansing : Null Removal */
    val clickStreamDataRead=FileReader.readFile(sparkSession,"csv",appConstants.Constants.ClickStreamPath)
    val nullRemovedData = cleanser.FileCleanser.nullRemovalClickStreamData(clickStreamDataRead,log)

    val itemDataRead=FileReader.readFile(sparkSession,"csv",appConstants.Constants.ItemDataPath)
    val nullRemovalFromItem= cleanser.FileCleanser.nullRemovalItemData(itemDataRead,log)

  }
}