package constants
object ApplicationConstants {
  val CLICK_STREAM_INPUT_PATH="data/clickstream_log.csv"
  val CLICK_STREAM_NOT_NULL_KEYS=Seq( "visitor_id","item_id")
  val CLICK_STREAM_NUMERIC = Seq("id", "session_id")
  val CLICK_STREAM_STRING = Seq("device_type", "redirection_source")
  val CLICK_STREAM_BOOLEAN = Seq("is_add_to_cart","is_order_placed")
  val CLICK_STREAM_TIME_STAMP = Seq("event_timestamp")


  val ITEM_DATA_INPUT_PATH="data/item_data.csv"
  val ITEM_DATA_NOT_NULL_KEYS=Seq("item_id")
  val ITEM_DATA_NUMERIC=Seq("item_price","vendor_id")
  val ITEM_DATA_STRING = Seq("product_type","department_name","vendor_name")

  val FILE_FORMAT = "csv"

  val CLICKS_STREAM_DATATYPE = Seq(("id", "long"),
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


  val clickstreamPrimaryCol="visitor_id"
  val itemPrimaryCol=Seq("item_id")
  //foreign key of clickstream data for item data(a table can have a single foreign key for respective another table,another foreign key would be for another table bcs a foreign key should have a primary key for another table & table can have only one primary key.)
  val CLICK_STREAM_FOREIGN_KEY=Seq("item_id")


}