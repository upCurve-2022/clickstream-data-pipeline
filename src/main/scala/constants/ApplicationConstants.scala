package constants

object ApplicationConstants {
  val CLICK_STREAM_INPUT_PATH="data/clickstream_log.csv"
  val CLICK_STREAM_NOT_NULL_KEYS=Seq( "visitor_id","item_id")
  val CLICK_STREAM_TIME_STAMP = Seq("event_timestamp")

  val COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP=Map("id"->(-1),"device_type"->"UNKNOWN","session_id"->"UNKNOWN","redirection_source"->"UNKNOWN","is_add_to_cart"->false,"is_order_placed"->false)

  val COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP=Map("item_price"->(-1),"product_type"->"UNKNOWN","department_name"->"UNKNOWN","vendor_id"->(-1),"vendor_name"->"UNKNOWN")


  val ITEM_DATA_INPUT_PATH="data/item_data.csv"
  val ITEM_DATA_NOT_NULL_KEYS=Seq("item_id")

  val FILE_FORMAT = "csv"

  val INPUT_TIME_STAMP_FORMAT = "MM/dd/yyyy HH:mm"


  val CLICK_STREAM_DATATYPE_LIST = List(
    ("id", "int"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean"))
  val ITEM_DATATYPE_LIST = List(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))

  //foreign key of clickstream data for item data(a table can have a single foreign key for respective another table,another foreign key would be for another table bcs a foreign key should have a primary key for another table & table can have only one primary key.)
  val CLICK_STREAM_FOREIGN_KEY=Seq("item_id")


}