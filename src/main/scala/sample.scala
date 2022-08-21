//import checks.DataQualityChecks.{schema, spark}
import cleanser.FileCleanser.{colDatatypeModifier, stringToTimestamp}
import constants.ApplicationConstants.{CLICK_STREAM_DATATYPE, INPUT_TIME_STAMP_FORMAT, TIME_STAMP_COL}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import utils.ApplicationUtils.createSparkSession
import scala.collection.JavaConversions._

object sample {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession()
    val df = spark.read.option("header", "true").format("csv").load("data/test.csv")
    var count = 0;
    var errorList = List[Row]()
    val errorSchema = StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("event_timestamp", TimestampType, nullable = true),
      StructField("device_type", StringType, nullable = true),
      StructField("session_id", StringType, nullable = true),
      StructField("visitor_id", StringType, true),
      StructField("item_id", StringType, true),
      StructField("redirection_source", StringType, true),
      StructField("is_add_to_cart", BooleanType, true),
      StructField("is_order_placed", BooleanType, true)))
    val convertedDF = stringToTimestamp(df, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)
    convertedDF.show()
    val modifiedClickStreamDF = colDatatypeModifier(convertedDF, CLICK_STREAM_DATATYPE)
    modifiedClickStreamDF.collect().foreach(row => {
      row.toSeq.foreach(c => {
        if (c == "null" || c == null) {
          println(c)
          count = count + 1
        }
      })
      println(count)
      println("-------")
      if (count > 1) {
        println("in")
        errorList = errorList :+ row
        count = 0
      }
    }
    )
//    val schema = StructType( Array(
//      StructField("language", StringType,true),
//      StructField("users", StringType,true)
//    ))
//    val rowData= Seq(Row("Java", "20000"),
//      Row("Python", "100000"),
//      Row("Scala", "3000"))
//    var dfFromData3 = spark.createDataFrame(rowData,schema)
    val errorDF = spark.createDataFrame(errorList, errorSchema)
    errorDF.show()
  }
}
