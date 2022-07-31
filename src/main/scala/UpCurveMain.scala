import pipeline.DataPipeline

object UpCurveMain {
  def main(args: Array[String]): Unit= {

    try{
      //performing reader and cleanser operations on click stream dataset
      DataPipeline.clickStreamDataModification()

      //performing reader and cleanser operations on item dataset
      //DataPipeline.itemDataModification()

    }catch {
      case ex : NoClassDefFoundError =>{
        println("Spark Session is not defined")
      }
      case ex1 : UnsupportedOperationException =>{

      }
    }


  }

}
