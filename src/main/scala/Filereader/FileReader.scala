package Filereader

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReader {

  def fileReader(spark: SparkSession, format: String, path: String): DataFrame = {

      val df: DataFrame = spark.read.option("header", "true").format(format).load(path)
      return df;

  }

}
