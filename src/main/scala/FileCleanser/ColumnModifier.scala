package FileCleanser
import org.apache.spark.sql.DataFrame
object ColumnModifier {
  def apply(seq:Seq[(String , String)],df: DataFrame): DataFrame = {
  val new_df=df.select (seq.map { case (c, t) => df.col(c).cast(t)}: _*)
    return  new_df
    }

}
