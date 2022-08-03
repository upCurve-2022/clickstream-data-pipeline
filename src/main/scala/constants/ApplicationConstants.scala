package constants

object ApplicationConstants {
  //input path
  val CLICK_STREAM_PATH = "spark.app.clickStreamInputPath"
  val ITEM_DATA_PATH = "spark.app.itemDataPath"

  val CLICK_STREAM_PRIMARY_KEYS : Seq[String] = Seq("visitor_id", "item_id")
  val ITEM_PRIMARY_KEYS : Seq[String] = Seq("item_id")

  //constants for null values handling
  val CLICK_STREAM_NOT_NULL_KEYS = Seq("id", "session_id","item_id")
  val ITEM_NOT_NULL_KEYS = Seq("item_id","vendor_id")

  val CLICK_STREAM_BOOLEAN = Seq("is_add_to_cart","is_order_placed")
  val ITEM_DATA_BOOLEAN = Seq()
  
  val CLICK_STREAM_TIMESTAMP = Seq("event_timestamp")
  val ITEM_DATA_TIMESTAMP = Seq()

  //output path
  val CLICK_STREAM_OUTPUT_PATH = "spark.app.clickStreamInputPath"
  val ITEM_OUTPUT_PATH = "spark.app.itemDataOutputPath"

  val INPUT_TIME_STAMP_FORMAT = "MM/dd/yyyy HH:mm"
  val TIME_STAMP_COL = "event_timestamp"
  val REDIRECTION_COL = "redirection_source"

  val FILE_FORMAT = "csv"

  val CLICK_STREAM_DATATYPE = List(
    ("id", "int"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean"))

  val ITEM_DATATYPE = List(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))

}
