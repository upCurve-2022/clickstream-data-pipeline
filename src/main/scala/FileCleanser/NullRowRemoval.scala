package FileCleanser

import org.apache.spark.sql.DataFrame

object NullRowRemoval {

  def apply(df:DataFrame):DataFrame = {
  val null_free_df:DataFrame=df.na.drop(Seq("id","event_timestamp","session_id","visitor_id","item_id","redirection_source"))
  return  null_free_df}
}
