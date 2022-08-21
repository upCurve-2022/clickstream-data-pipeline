package cleanser

import constants.ApplicationConstants.COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import service.FileReader

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class NullHandlingTest extends AnyFlatSpec{

  implicit val spark=utils.ApplicatonUtils.sparkSessionCall()

  "removeRows method " should "remove null rows" in{
    val clickStreamDataRead=FileReader.readFile(spark,constants.ApplicationConstants.FILE_FORMAT,constants.ApplicationConstants.CLICK_STREAM_INPUT_PATH)

    val timeStampDataTypeDF=datatype.DataType.stringToTimestamp(clickStreamDataRead,"event_timestamp",constants.ApplicationConstants.INPUT_TIME_STAMP_FORMAT)
    val changeDataTypeDF=datatype.DataType.colDatatypeModifier(timeStampDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_DATATYPE_LIST)

    val modifiedDF=cleanser.FileCleanser.removeRows(changeDataTypeDF,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
    modifiedDF.show()

    val visitorNullCount=modifiedDF.filter(col("visitor_id").isNull).count()
    val itemIdNullCount=modifiedDF.filter(col("item_id").isNull).count()
    assert(visitorNullCount==0 && itemIdNullCount==0)

  }
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

    val modifiedDF=cleanser.FileCleanser.removeRows(inputDF1,constants.ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)
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
    val changeDataTypeDF=datatype.DataType.colDatatypeModifier(inputDF2,constants.ApplicationConstants.CLICK_STREAM_DATATYPE_LIST)
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


   val modifiedDF3=cleanser.FileCleanser.fillCurrentTime(inputDF3,Seq("event_timestamp"))

    val expectedData=Seq(
      Row("30334","2020-11-15 15:27:01","android","B000078","I7099","B29093","Youtube","FALSE","FALSE"),
      Row("30385",DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now),"android","B000078","I7099","D8142","google","FALSE","FALSE"),
      Row("30503","2020-11-15 15:27:01","android","B000078","I7099","D8142","google","FALSE","FALSE")
    )


    val expectedSchema=StructType(Array(

      StructField("id", StringType),
      StructField("event_timestamp",StringType),
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

}
