package md.fusionworks.vardictspark.spark

import md.fusionworks.vardictspark.config.ConfigLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkContextFactory {
  private var sparkContext: Option[SparkContext] = None
  private lazy val sparkSqlContext = new SQLContext(getSparkContext())

  def getSparkContext(master: Option[String] = None): SparkContext = {
    sparkContext match {
      case Some(context) => context
      case None =>
        val sparkConf = new SparkConf().setAppName("JBrowse-ADAM")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.driver.memory", "10g")
        .set("spark.kryoserializer.buffer.max", "500m")
        .set("spark.driver.maxResultSize", "8g")

        master match {
          case Some(url) => sparkConf.setMaster(url)
          case None =>
        }

        sparkContext = Some(new SparkContext(sparkConf))
        sparkContext.get
    }
  }

  def startSparkContext(): Unit = {
    println("Starting SparkContext...")
    getSparkContext()
  }

  def getSparkSqlContext = sparkSqlContext

  def stopSparkContext() = getSparkContext().stop()
}
