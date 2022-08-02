
import org.apache.log4j.Logger

import scala.util.Try

object Clickstream {

  val log:Logger=Logger.getLogger(getClass)
  def main(args: Array[String]) {
/*
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("DataEngineeringUpCurve")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    /*Data Cleansing : Null Removal */
    val clickStreamDataRead=FileReader.readFile(sparkSession,"csv",constants.ApplicationConstants.ClickStreamPath)
    val nullRemovedData = cleanser.FileCleanser.nullRemovalClickStreamData(clickStreamDataRead,log)

    val itemDataRead=FileReader.readFile(sparkSession,"csv",constants.ApplicationConstants.ItemDataPath)
    val nullRemovalFromItem= cleanser.FileCleanser.nullRemovalItemData(itemDataRead,log)
*/
    try{
      service.DataPipeline.execute()
    }
    catch
    {
      case exception: Exception=>println("Exception found : "+exception)
    }
  }
}