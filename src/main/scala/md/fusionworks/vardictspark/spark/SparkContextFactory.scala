package md.fusionworks.vardictspark.spark

import org.apache.spark.{SparkConf, SparkContext}


object SparkContextFactory {
  private var sparkContext: Option[SparkContext] = None

  def getSparkContext(master: Option[String] = None): SparkContext = {
    sparkContext match {
      case Some(context) => context
      case None =>
        val sparkConf = new SparkConf().setAppName("VarDictSpark")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //        .set("spark.kryoserializer.buffer.max", "500m")
        //        .set("spark.driver.maxResultSize", "8g")

        master match {
          case Some(url) => sparkConf.setMaster(url)
          case None =>
        }

        sparkContext = Some(new SparkContext(sparkConf))
        sparkContext.get
    }
  }

}
