package constants

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object ApplicationConstants {
  /** **************** application specifications **************** **/
  //spark.app key
  val APP_NAME = "spark.app.name"
  val APP_MASTER = "spark.app.master"

  //primary keys
  val CLICK_STREAM_PRIMARY_KEYS: Seq[String] = Seq("session_id", "item_id")
  val ITEM_PRIMARY_KEYS: Seq[String] = Seq("item_id")
  val FINAL_PRIMARY_KEY = Seq("session_id", "item_id")

  //final table schema
  val SCHEMA_PATH = "spark.app.schemaPath"

  /** **************** reader constants **************** **/
  val CLICK_STREAM_INPUT_PATH: String = "spark.app.clickStreamInputPath"
  val ITEM_DATA_INPUT_PATH: String = "spark.app.itemDataInputPath"
  val READ_FORMAT = "csv"

  /** **************** null handling constants **************** **/
  //for click stream data
  val COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP = Map(
    "id" -> (-1),
    "event_timestamp" -> "1999-01-01 00:00:00",
    "device_type" -> "unknown",
    "session_id" -> "unknown",
    "redirection_source" -> "unknown",
    "is_add_to_cart" -> false,
    "is_order_placed" -> false)

  //for item data
  val COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP = Map(
    "item_price" -> (-1),
    "product_type" -> "unknown",
    "department_name" -> "unknown",
    "vendor_id" -> (-1),
    "vendor_name" -> "unknown")

  /** **************** datatype modification constants **************** **/
  //for click stream data
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

  //for item data
  val ITEM_DATATYPE = List(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))

  /** **************** join and transform constants **************** **/
  //join constants
  val JOIN_KEY: Seq[String] = Seq("item_id")
  val JOIN_TYPE: String = "left"

  //transform constants - date
  val EVENT_DATE = "event_d"
  val DATE_FORMAT = "yyyy-MM-dd"

  //transform constants - load time
  val RECORD_LOAD_TIME = "record_load_ts"
  val OUTPUT_TIME_STAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

  /** **************** database constants **************** **/
  //for database connectivity
  val WRITE_FORMAT = "jdbc"
  val JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

  //for authentication
  val DB_USER = "root"
  val ENCRYPTED_DATABASE_PASSWORD: String = "data/encrypted_password.txt"

  //for final table write
  val DATABASE_URL: String = "spark.app.databaseURL"
  val TABLE_NAME = "final_table"

  //for error tables write
  val ERR_TABLE_NULL_CHECK = "error_table_nullCheck"
  val ERR_TABLE_DUP_CHECK = "error_table_duplicateCheck"

  /** **************** miscellaneous constants **************** **/
  val TIME_STAMP_COL = "event_timestamp"
  val REDIRECTION_COL = "redirection_source"
  val INPUT_TIME_STAMP_FORMAT = "MM/dd/yyyy HH:mm"
}
