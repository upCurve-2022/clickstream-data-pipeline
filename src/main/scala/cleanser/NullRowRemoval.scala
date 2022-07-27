package cleanser

import org.apache.spark.sql.DataFrame

object NullRowRemoval {

  def apply(inputDF:DataFrame):DataFrame = {
  val outputDF:DataFrame=inputDF.na.drop(Seq("id","session_id","item_id"))
    outputDF}
}
