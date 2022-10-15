package cleanser

import cleanser.FileCleanser.{removeDuplicates, stringToTimestamp}
import constants.ApplicationConstants
import constants.ApplicationConstants._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class FileCleanserTest extends AnyFlatSpec {
  implicit val spark: SparkSession = helper.Helper.createSparkSession()

  import spark.implicits._

  //  test cases for remove null
  "removeRows method" should "remove null rows" in {
    val inputDF = Seq(
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", null, "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", null, "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val modifiedDF = cleanser.FileCleanser.removeRows(inputDF, CLICK_STREAM_PRIMARY_KEYS)

    val expectedDF = Seq(
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("29839", "11/15/2020 15:27", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val result = modifiedDF.except(expectedDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)

  }

  //  test cases for fill values
  "fillValues method " should "fill null values " in {

    val inputDF = Seq(
      ("30334", "11/15/2020 15:23", null, "B000078", "I7099", "B29093", "Youtube", "", ""),
      ("", null, "android", "B000078", null, "D8142", "google", "TRUE", ""),
      ("30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", null, "TRUE", "TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val timestampDataTypeDF = stringToTimestamp(inputDF, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF = cleanser.FileCleanser.colDatatypeModifier(timestampDataTypeDF,CLICK_STREAM_DATATYPE)
    val modifiedDF = cleanser.FileCleanser.fillValues(changeDataTypeDF, COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    val expectedData = Seq(
      Row(30334,Timestamp.valueOf("2020-11-15 15:23:00"), "unknown", "B000078", "I7099", "B29093", "Youtube", false, false),
      Row(-1, Timestamp.valueOf("1999-01-01 00:00:00"), "android", "B000078", "unknown", "D8142", "google", true, false),
      Row(30503,Timestamp.valueOf("2020-11-15 15:27:00"), "android", "B000078", "I7099", "D8142", "unknown", true, true)
    )

    val expectedSchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("event_timestamp", TimestampType),
      StructField("device_type", StringType),
      StructField("session_id", StringType),
      StructField("visitor_id", StringType),
      StructField("item_id", StringType),
      StructField("redirection_source", StringType),
      StructField("is_add_to_cart", BooleanType),
      StructField("is_order_placed", BooleanType)
    ))

    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    val result = modifiedDF.except(expectedDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  //test cases for string to timestamp method
  "stringToTimeStamp method " should "convert string to timestamp format" in {

    val inputDF = Seq((
      "30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "01/20/2020 15:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931", "11/15/2020 9:07", "android", "B000092", "C2146", "H6156", "facebook", "", ""),
      ("13931", "11/15/2020 19:07", "android", "", "C2146", "", "facebook", "", "")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id ",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )

    val modifiedDf: DataFrame = cleanser.FileCleanser.stringToTimestamp(inputDF, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)

    val expectedDF: DataFrame = Seq((
      "30503", "2020-11-15 15:27:00", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931", "2020-11-15 09:07:00", "android", "B000092", "C2146", "H6156", "facebook", "", ""),
      ("13931", "2020-11-15 19:07:00", "android", "", "C2146", "", "facebook", "", "")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id ",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )

    val result = modifiedDf.except(expectedDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  //  test cases for toLower case method
  "toLowerCase method" should "convert redirectionSource column value to lowercase" in {
    val inputDF = Seq((
      "30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "01/20/2020 15:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931", "11/15/2020 9:07", "android", "B000092", "C2146", "H6156", "facebook", "", ""),
      ("13931", "11/15/2020 19:07", "android", "", "C2146", "", "facebook", "", "")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id ",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )

    val modifiedDf: DataFrame = cleanser.FileCleanser.toLowercase(inputDF, REDIRECTION_COL)

    val expectedDF: DataFrame = Seq((
      "30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "facebook", "TRUE", "TRUE"),
      ("30542", "01/20/2020 15:00", "android", "B000078", "I7099", "D8142", "google", "TRUE", "TRUE"),
      ("13931", "11/15/2020 9:07", "android", "B000092", "C2146", "H6156", "facebook", "", ""),
      ("13931", "11/15/2020 19:07", "android", "", "C2146", "", "facebook", "", "")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id ",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )

    val result = modifiedDf.except(expectedDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  //  test cases for column datatype modifier
  "columnDatatypeModifier" should "convert column datatype to required format" in {
    val inputDF = Seq((
      "30503", "2020-11-15 15:27:00", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931", "2020-11-15 09:07:00", "android", "B000092", "C2146", "H6156", "facebook", "", ""),
      ("13931", "2020-11-15 19:07:00", "android", "", "C2146", "", "facebook", "", "")
    ).toDF(
      "id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )

    val outputDF = cleanser.FileCleanser.colDatatypeModifier(inputDF, constants.ApplicationConstants.CLICK_STREAM_DATATYPE)

    val event_timestamp_col = outputDF.schema("event_timestamp").dataType.typeName === "timestamp"
    val add_to_cart_col = outputDF.schema("is_add_to_cart").dataType.typeName === "boolean"
    val is_order_placed_col = outputDF.schema("is_add_to_cart").dataType.typeName === "boolean"
    assertResult(expected = true)(event_timestamp_col)
    assertResult(expected = true)(add_to_cart_col)
    assertResult(expected = true)(is_order_placed_col)
  }

  //test cases for  removeDuplicates method
  "removeDuplicates" should "remove duplicates to required format" in {
    val clickStreamDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B19304", "LinkedIn", "", "TRUE"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B29093", "Youtube", "", ""),
      ("30385", "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google", "TRUE", ""),
      ("30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE")
    ).toDF(
      "id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val modifiedClickStreamDF: DataFrame = removeDuplicates(helper.Helper.DATABASE_TEST_URL, clickStreamDF, CLICK_STREAM_PRIMARY_KEYS, Some(ApplicationConstants.TIME_STAMP_COL))

    val expectedClickStreamDF: DataFrame = Seq(
      ("29839", "11/15/2020 15:11", "android", "B000078", "I7099", "B17543", "GOOGLE", "", "TRUE"),
      ("30504", "11/15/2020 15:27", "android", "B000078", "I7099", "B19304", "LinkedIn", "", "TRUE"),
      ("30334", "11/15/2020 15:23", "android", "B000078", "I7099", "B29093", "Youtube", "", ""),
      ("30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE")
    ).toDF(
      "id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val itemDF : DataFrame = Seq(
      ("C6880", "2301", "D040", "Computers & Accessories", "3", "MOJO INC"),
      ("F4939" ,"1756.5" ,"G822" ,"Collectibles", "2", "AMBER PRODUCTS"),
      ("F4939" ,"1756.5" ,"G822" ,"Collectibles", "2", "AMBER"),
      ("E0383", "412.5", "B619", "Apps & Games", "4", "LARVEL SUPPLY"),
      ("I777", "1177.5", "F264", "Baby", "2", "AMBER PRODUCTS")
    ).toDF(
      "item_id",
      "item_price",
      "product_type",
      "department_name",
      "vendor_id",
      "vendor_name")

    val modifiedItemDF : DataFrame = removeDuplicates(helper.Helper.DATABASE_TEST_URL, itemDF, ITEM_PRIMARY_KEYS, None)

    val expectedItemDF : DataFrame = Seq(
      ("C6880", "2301", "D040", "Computers & Accessories", "3", "MOJO INC"),
      ("F4939" ,"1756.5" ,"G822" ,"Collectibles", "2", "AMBER PRODUCTS"),
      ("E0383", "412.5", "B619", "Apps & Games", "4", "LARVEL SUPPLY"),
      ("I777", "1177.5", "F264", "Baby", "2", "AMBER PRODUCTS")
    ).toDF(
      "item_id",
      "item_price",
      "product_type",
      "department_name",
      "vendor_id",
      "vendor_name")

    val clickStreamResult = modifiedClickStreamDF.except(expectedClickStreamDF)
    val clickStreamAns = clickStreamResult.count()
    assertResult(0)(clickStreamAns)

    val itemResult = modifiedItemDF.except(expectedItemDF)
    val itemAns = itemResult.count()
    assertResult(0)(itemAns)

  }
}




