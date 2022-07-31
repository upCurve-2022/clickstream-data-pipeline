package constants

object AppConstants {
  val CLICK_STREAM_PATH = "data/clickstream_log.csv"
  val ITEM_DATA_PATH = "data/item_data.csv"

  val CLICK_STREAM_PRIMARY_KEYS : Seq[String] = Seq("visitor_id", "item_id")
  val ITEM_PRIMARY_KEYS : Seq[String] = Seq("item_id")

  val CLICK_STREAM_NOT_NULL_KEYS = Seq("id", "session_id","item_id")
  val ITEM_NOT_NULL_KEYS = Seq("item_id","vendor_id")

  val CLICK_STREAM_OUTPUT_PATH = "data/output_data/clickstream_data.csv"
  val ITEM_OUTPUT_PATH = "data/output_data/item_data.csv"

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
