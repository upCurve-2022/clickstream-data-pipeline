package constants
object ApplicationConstants {
  val ClickStreamPath="data/clickstream_log.csv"
  val ItemDataPath="data/item_data.csv"

  val ClickStreamPrimary=Seq("id", "session_id","item_id")
  val ItemDataPrimary=Seq("item_id","vendor_id")

  val DataTypeClickStream = Seq(("id", "long"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean"))

  val DataTypeItem = List(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))

  val ClickStreamBoolean = Seq("is_add_to_cart","is_order_placed")
  val ItemDataBoolean = Seq()

  val clickStreamTimestamp = Seq("event_timestamp")
  val ItemDataTimestamp = Seq()
}