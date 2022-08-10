import constants.ApplicationConstants.{INPUT_TIME_STAMP_FORMAT, TIME_STAMP_COL}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

import scala.tools.nsc.javac.JavaTokens.TRUE

class FileCleanserTest extends AnyFlatSpec {
  implicit val spark = utils.ApplicationUtils.createSparkSession()

  import spark.implicits._
  "event_timestamp" should "be in timestamp format" in {

    val inputDF = Seq((
      "30503", "11/15/2020 15:27", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "01/20/2020 15:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931","11/15/2020 9:07","android","B000092","C2146","H6156","facebook","",""),
      ("13931","11/15/2020 19:07","android","","C2146","","facebook","","")
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
    val modifiedDf:DataFrame=cleanser.FileCleanser.stringToTimestamp(inputDF,TIME_STAMP_COL,INPUT_TIME_STAMP_FORMAT)
    val expectedDF :DataFrame= Seq((
      "30503", "2020-11-15 15:27:00", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931","2020-11-15 09:07:00","android","B000092","C2146","H6156","facebook","",""),
      ("13931","2020-11-15 19:07:00","android","","C2146","","facebook","","")
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
    val result=modifiedDf.except(expectedDF)
    val ans=result.count()
    val count=0;
    assertResult(count)(ans)
    //    assert(count==ans)
  }


  "event_" should "be in timestamp format" in {
    val input2DF = Seq((
      "30503", "2020-11-15 15:27:00", "android", "B000078", "I7099", "D8142", "FACEBOOK", "TRUE", "TRUE"),
      ("30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "Google", "TRUE", "TRUE"),
      ("13931","2020-11-15 09:07:00","android","B000092","C2146","H6156","facebook","",""),
      ("13931","2020-11-15 19:07:00","android","","C2146","","facebook","","")
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
    val outputDF=cleanser.FileCleanser.colDatatypeModifier(input2DF,constants.ApplicationConstants.CLICK_STREAM_DATATYPE)
    //    val clickStreamSchema=StructType(Array(
    //      StructField("id", IntegerType,true),
    //      StructField("event_timestamp", TimestampType,true),
    //      StructField("device_type", StringType,true),
    //      StructField("session_id", StringType,true),
    //      StructField("visitor_id", StringType,true),
    //      StructField("item_id", StringType,true),
    //      StructField("redirection_source",StringType,true),
    //      StructField("is_add_to_cart", BooleanType,true),
    //      StructField("is_order_placed", BooleanType,true)
    //    ))
    val output2DF = Seq((
      "30503", "2020-11-15 15:27:00", "android", "B000078", "I7099", "D8142", "FACEBOOK", TRUE, TRUE),
      ("30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "Google", TRUE, TRUE),
      ("13931","2020-11-15 09:07:00","android","B000092","C2146","H6156","facebook","",""),
      ("13931","2020-11-15 19:07:00","android","","C2146","","facebook","","")
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

    val result=output2DF.except(outputDF)
    val ans=result.count()
    val count=0;
    assertResult(count)(ans)
    ////    val result=clickStreamSchema
    ////    val s1 = clickStreamSchema.fields.map(f => (f.name, f.nullable))
    ////    val s2 = outputDF.schema.fields.map(f => (f.name, f.nullable))
    ////
    ////    val res = s1 zip s2 forall {
    ////      case ((f1, n1), (f2,n2)) => f1 == f2 && n1 ==  n2
    ////    }
    //  val a=clickStreamSchema.equals(outputDF.schema)
    //    assertResult(a==true)(0)

  }

}

