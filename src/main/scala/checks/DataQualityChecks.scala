package checks

import exceptions.Exceptions.{DuplicateValuesExistException, NullValuesExistException, SchemaValidationFailedException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object DataQualityChecks {

  //null check
  def nullCheck(inputDF : DataFrame, columns : List[String]): List[Unit] = {
    columns.map(c => {
      if(inputDF.filter(inputDF(c).isNull
        || inputDF(c) === ""
        || inputDF(c).contains("NULL")
        || inputDF(c).contains("null")).count() != 0){
        throw NullValuesExistException("Null values are present int the dataset")
      }
    })
  }

  //duplicates check
  def duplicatesCheck(inputDF: DataFrame, primaryKeyCols : Seq[String], orderByCol: Option[String]) : Unit = {
    orderByCol match {
      case Some(column) =>
        inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
        if(col("rn") != 1){
          throw DuplicateValuesExistException("Duplicates found in click stream dataset")
        }
      case None =>
        if(inputDF.except(inputDF.dropDuplicates(primaryKeyCols)).count() != 0){
          throw DuplicateValuesExistException("Duplicates found in item dataset")
        }
    }
  }

  //schema validation
  def schemaValidationCheck(inputDF : DataFrame): Unit ={
    inputDF.schema.fields.foreach(f=> {
      inputDF.select(f.name).foreach(c => {
        if(!c(0).getClass.getName.toLowerCase.contains(f.dataType.toString.toLowerCase().split("type")(0))){
          throw SchemaValidationFailedException(c(0) + " does not have the same datatype as its column " + f.name)
        }
      })
    })
  }

  //categorical

}
