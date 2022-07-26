package AppConstants

object Constants {
  val CLICK_STREAM_PATH = "C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\clickstream_log.csv"
  val ITEM_DATA_PATH = "C:/Users/DELL/Desktop/sparkAssignment/de/clickstream-data-pipeline/data/item_data.csv"
  val colDataType_click_stream = Seq(
    ("id", "int"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean")
  )
val colDataType_item_data=Seq(
  ("item_id","string"),
  ("item_price","double"),
  ("product_type","string"),
  ("department_name","string"),
  ("vendor_id","int"),
  ("vendor_name","string")
)

}
