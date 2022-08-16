package checks

import exceptions.Exceptions.{NullValuesExistException, SchemaValidationFailedException}
import org.apache.spark.sql.DataFrame

object DataQualityChecks {

  //nulls
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

  //duplicates

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
