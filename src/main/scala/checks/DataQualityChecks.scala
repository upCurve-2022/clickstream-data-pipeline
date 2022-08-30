package checks

import constants.ApplicationConstants.{ERR_TABLE_NULL_CHECK, TABLE_NAME}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import service.FileWriter
import scala.collection.JavaConversions._

object DataQualityChecks {

  var count = 0
  var errorList: List[Row] = List[Row]()
  val errorSchema: StructType = StructType(Array(
    StructField("item_id", StringType, nullable = true),
    StructField("id", IntegerType, nullable = true),
    StructField("event_timestamp", TimestampType, nullable = true),
    StructField("device_type", StringType, nullable = true),
    StructField("session_id", StringType, nullable = true),
    StructField("visitor_id", StringType, nullable = true),
    StructField("redirection_source", StringType, nullable = true),
    StructField("is_add_to_cart", BooleanType, nullable = true),
    StructField("is_order_placed", BooleanType, nullable = true),
    StructField("item_price", DoubleType, nullable = true),
    StructField("product_type", StringType, nullable = true),
    StructField("department_name", StringType, nullable = true),
    StructField("vendor_id", IntegerType, nullable = true),
    StructField("vendor_name", StringType, nullable = true),
    StructField("event_d", DateType, nullable = true),
    StructField("record_load_ts", TimestampType, nullable = true)))

  //null data quality check
  def nullCheck(databaseUrl: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    var errorList: List[Row] = List[Row]()
    inputDF.collect().foreach(row => {
      row.toSeq.foreach(c => {
        if (c == "unknown" || c == -1 || c == false || c == "null" || c == "NULL" || c == "" || c == null) {
          count = count + 1
        }
      })
      if (count > 9) {
        errorList = errorList :+ row
      }
      count = 0
    })

    val errorDF = spark.createDataFrame(errorList, errorSchema)
    FileWriter.fileWriter(databaseUrl, ERR_TABLE_NULL_CHECK, errorDF)
    val nullCheckFinalDF = inputDF.except(errorDF)
    nullCheckFinalDF
  }

  //duplicates data quality check
  def duplicatesCheck(databaseUrl: String, inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: String): DataFrame = {
    val exceptionsDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(orderByCol))))
      .filter(col("rn") > 1).drop("rn")

    val duplicateCheckFinalDF = inputDF.except(exceptionsDF)
    FileWriter.fileWriter(databaseUrl, TABLE_NAME, exceptionsDF)
    duplicateCheckFinalDF
  }


}
