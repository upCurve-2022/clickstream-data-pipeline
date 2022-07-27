package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReader {

  def fileReader(spark: SparkSession, format: String, path: String): DataFrame = {

      val outputDF: DataFrame = spark.read.option("header", "true").format(format).load(path)
   outputDF;

  }

}
