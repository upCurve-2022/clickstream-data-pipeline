import pipeline.DataPipeline

object UpCurveMain {
  def main(args: Array[String]): Unit= {

    //performing reader and cleanser operations on click stream dataset
    DataPipeline.clickStreamDataModification()

    //performing reader and cleanser operations on item dataset
    DataPipeline.itemDataModification()

  }

}
