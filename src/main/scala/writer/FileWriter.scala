package writer

import exceptions.ApplicationException.FileWriterException
import org.apache.spark.sql.DataFrame


object FileWriter {

  def fileWriter(inputDF:DataFrame,format:String,path:String):Unit={
    try{
      inputDF.repartition(1).write.format(format).option("header","true").mode("overwrite").save(path)
    }catch {
      case e:Exception=> throw  FileWriterException("Error in writing file to given location")
    }
  }

}
