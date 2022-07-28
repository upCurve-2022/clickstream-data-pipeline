package appConstants
object Constants {
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
}