package cleanser

import constants.ApplicationConstants._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


class FileCleanserTest extends AnyFlatSpec {
  implicit val spark :SparkSession= helper.Helper.createSparkSession()

  import spark.implicits._
  "removeRows method1" should "remove null rows" in{
    val inputDF1=Seq(
      ("29839","11/15/2020 15:27","android","B000078","I7099",null,"GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078",null,"B17543","GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")

    val modifiedDF=cleanser.FileCleanser.removeRows(inputDF1,CLICK_STREAM_NOT_NULL_KEYS)
    modifiedDF.show()

    val expectedDF1=Seq(
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE"),
      ("29839","11/15/2020 15:27","android","B000078","I7099","B17543","GOOGLE","","TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")


    val result=modifiedDF.except(expectedDF1)
    val ans=result.count()
    val count=0
    assertResult(count)(ans)

  }
  "fillValues method " should "fill null values " in {

    val inputDF2=Seq(
      ("30334","11/15/2020 15:23",null,"B000078","I7099","B29093","Youtube","",""),
      ("","11/15/2020 15:24","android",null,"I7099","D8142","google","TRUE",""),
      ("30503","11/15/2020 15:27","android","B000078","I7099","D8142",null,"TRUE","TRUE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")


    //    val timeStampDataTypeDF=datatype.DataType.stringToTimestamp(inputDF2,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF=cleanser.FileCleanser.colDatatypeModifier(inputDF2,constants.ApplicationConstants.CLICK_STREAM_DATATYPE)
    val modifiedDF2=cleanser.FileCleanser.fillValues(changeDataTypeDF,COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)
    //val modifiedDF2=cleanser.FileCleanser.fillValues(changeDataTypeDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)


    modifiedDF2.show()

    val expectedData=Seq(
      Row(30334,null,"UNKNOWN","B000078","I7099","B29093","Youtube",false,false),
      Row(-1,null,"android","UNKNOWN","I7099","D8142","google",true,false),
      Row(30503,null,"android","B000078","I7099","D8142","UNKNOWN",true,true)
    )

    val expectedSchema=StructType(Array(

      StructField("id", IntegerType),
      StructField("event_timestamp",StringType),
      StructField("device_type", StringType),
      StructField("session_id", StringType),
      StructField("visitor_id", StringType),
      StructField("item_id", StringType),
      StructField("redirection_source", StringType),
      StructField("is_add_to_cart", BooleanType),
      StructField("is_order_placed",BooleanType)
    ))

    val expectedDF2 = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),expectedSchema)

    expectedDF2.show()

    val result=modifiedDF2.except(expectedDF2)
    val ans=result.count()
    val count=0
    assertResult(count)(ans)


  }
//    "fillValues method " should "fill null values " in {
//
//      val inputDataSet=Seq(
//        Row(30334,Timestamp.valueOf("11-15-2020 15:23"),null,"B000078","I7099","B29093","Youtube","",""),
//        Row(,Timestamp.valueOf("11-15-2020 15:24"),"android",null,"I7099","D8142","google","TRUE",""),
//       Row(30503,Timestamp.valueOf("11-15-2020 15:27"),"android","B000078","I7099","D8142",null,"TRUE","TRUE")
//      )
////        .toDF("id",
////        "event_timestamp",
////        "device_type",
////        "session_id",
////        "visitor_id",
////        "item_id",
////        "redirection_source",
////        "is_add_to_cart",
////        "is_order_placed")
//
//
//
//      val expectedSchema=List(
//        StructField("id", IntegerType,true),
//        StructField("event_timestamp",TimestampType,true),
//        StructField("device_type", StringType,true),
//        StructField("session_id", StringType,true),
//        StructField("visitor_id", StringType,true),
//        StructField("item_id", StringType,true),
//        StructField("redirection_source", StringType,true),
//        StructField("is_add_to_cart", BooleanType,true),
//        StructField("is_order_placed",BooleanType,true)
//      )
//
//      val inputDf=spark.createDataFrame(
//        spark.sparkContext.parallelize(inputDataSet),StructType(expectedSchema)
//      )
//
//
//
//
//      //    val timeStampDataTypeDF=datatype.DataType.stringToTimestamp(inputDF2,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
////      val changeDataTypeDF=cleanser.FileCleanser.colDatatypeModifier(inputDf,constants.ApplicationConstants.CLICK_STREAM_DATATYPE)
//      val modifiedDF2=cleanser.FileCleanser.fillValues(inputDf,COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)
//      //val modifiedDF2=cleanser.FileCleanser.fillValues(changeDataTypeDF,constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)
//
//
//      modifiedDF2.show()
//
//      val expectedData=Seq(
//        Row(30334,null,"UNKNOWN","B000078","I7099","B29093","Youtube",false,false),
//        Row(-1,null,"android","UNKNOWN","I7099","D8142","google",true,false),
//        Row(30503,null,"android","B000078","I7099","D8142","UNKNOWN",true,true)
//      )
//
//
//
//      val expectedDF2 = spark.createDataFrame(
//        spark.sparkContext.parallelize(expectedData),StructType(expectedSchema)
//      )
////
////      expectedDF2.show()
//
//      val result=modifiedDF2.except(expectedDF2)
//      val ans=result.count()
//      val count=0
//      assertResult(count)(ans)
//
//
//    }
  "fillCurrentTime method " should "fill null values " in {

    val inputDF3=Seq(
      ("30334","2020-11-15 15:27:01","android","B000078","I7099","B29093","Youtube","FALSE","FALSE"),
      ("30385",null,"android","B000078","I7099","D8142","google","FALSE","FALSE"),
      ("30503","2020-11-15 15:27:01","android","B000078","I7099","D8142","google","FALSE","FALSE")
    ).toDF("id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed")


    val modifiedDF3=cleanser.FileCleanser.fillCurrentTime(inputDF3)

    val expectedData=Seq(
      Row("30334","2020-11-15 15:27:01","android","B000078","I7099","B29093","Youtube","FALSE","FALSE"),
      Row("30385",DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now),"android","B000078","I7099","D8142","google","FALSE","FALSE"),
      Row("30503","2020-11-15 15:27:01","android","B000078","I7099","D8142","google","FALSE","FALSE")
    )


    val expectedSchema=StructType(Array(

      StructField("id", StringType),
      StructField("event_timestamp", TimestampType),
      StructField("device_type", StringType),
      StructField("session_id", StringType),
      StructField("visitor_id", StringType),
      StructField("item_id", StringType),
      StructField("redirection_source", StringType),
      StructField("is_add_to_cart", StringType),
      StructField("is_order_placed",StringType)
    ))

    val expectedDF3= spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),expectedSchema)


    val currTimeExpected=expectedDF3.take(2)(1)(1).toString.substring(0,13)
    val currTimeFromFunction=modifiedDF3.take(2)(1)(1).toString.substring(0,13)

    assert(currTimeFromFunction==currTimeExpected)

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
    //    assert(count==ans)
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
    //    assert(count==ans)

  }
  //  test cases for column datatype modifier
  "columnDatatypeModifier" should "convert column datatype to required format" in {
    val input2DF = Seq((
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
    val outputDF = cleanser.FileCleanser.colDatatypeModifier(input2DF, constants.ApplicationConstants.CLICK_STREAM_DATATYPE)

    val event_timestamp_col = outputDF.schema("event_timestamp").dataType.typeName === "timestamp"
    val add_to_cart_col = outputDF.schema("is_add_to_cart").dataType.typeName === "boolean"
    val is_order_placed_col = outputDF.schema("is_add_to_cart").dataType.typeName === "boolean"
    assertResult(expected = true)(event_timestamp_col)
    assertResult(expected = true)(add_to_cart_col)
    assertResult(expected = true)(is_order_placed_col)
  }
  //  test cases for joined table

}




