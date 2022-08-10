package constants

object Constants {
//Spark conf


  //  DataSet
  val CLICK_STREAM_INPUT_PATH = "C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\clickstream_log.csv"
  val ITEM_DATA_INPUT_PATH = "C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\item_data.csv"
  val CLICK_STREAM_OUTPUT_PATH="C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\output_clickStream"
  val ITEM_DATA_OUTPUT_PATH="C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\output_Item"
val JOIN_TABLE_PATH="C:\\Users\\DELL\\Desktop\\sparkAssignment\\de\\clickstream-data-pipeline\\data\\joined_table"
//  Format of file
  val CSV_FORMAT="csv"
  val JSON_FORMAT="json"
//Column of click_stream dataset

  val EVENT_TIMESTAMP="event_timestamp"
  val REDIRECTION_SOURCE="redirection_source"
  //  DataSet Columns and there DataType
  val CLICK_STREAM_DATATYPE = List(
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
  val ITEM_DATATYPE = Seq(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string")
  )
  //  Primary key
  val clickStream_foriegn_keys:Seq[String]=Seq("item_id")
  val clickStream_primary_keys: Seq[String] = Seq("visitor_id", "item_id")
  val item_primary_keys: Seq[String] = Seq("item_id")
}
