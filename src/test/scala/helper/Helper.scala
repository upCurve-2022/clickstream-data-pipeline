package helper

import org.apache.spark.sql.SparkSession

object Helper {
  val DATABASE_TEST_URL = "jdbc:mysql://localhost:3306/target_project_test"
  val CLICK_STREAM_TEST_INPUT_PATH = "data/test.csv"

  def createSparkSession(): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("UpCurve Data Pipeline Test")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate())
    spark
  }

}
