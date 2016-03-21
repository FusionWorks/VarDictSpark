package md.fusionworks.vardictspark

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}
import org.bdgenomics.adam.rdd.ADAMContext._


object Implicits {

  implicit def toVarDictSparkFunctions(sc: SparkContext): VarDictSparkFunctions = new VarDictSparkFunctions(sc)

  implicit def toCartesianFastaFunctions(rdd:RDD[(Region,Nucleotide)]): CartesianFastaFunctions =
    new CartesianFastaFunctions(rdd)
  implicit def toCartesianSamFunctions(rdd:RDD[(Region,SAMRecord)]): CartesianSamFunctions =
    new CartesianSamFunctions(rdd)
}


class VarDictSparkFunctions(sc: SparkContext) {

  def loadSamRecordRDD(filePath: String): RDD[SAMRecord] = {
    val job = newJob(sc.hadoopConfiguration)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job)).map(_._2.get())
    records
  }

  def loadRegions(path: String): RDD[Region] = {
    sc.textFile(path).map(Region.fromString)
  }

  def loadFasta(path: String): RDD[Nucleotide] = {
    sc.loadSequence(path)
      .flatMap { fragment =>
        fragment.getFragmentSequence.zipWithIndex.map { t =>
          Nucleotide(fragment.getContig.getContigName, (t._2 + fragment.getFragmentStartPosition + 1).toInt, t._1)
        }
      }
  }


  def newJob(config: Configuration): Job = {
    val jobClass: Class[_] = Class.forName("org.apache.hadoop.mapreduce.Job")
    try {
      // Use the getInstance method in Hadoop 2
      jobClass.getMethod("getInstance", classOf[Configuration]).invoke(null, config).asInstanceOf[Job]
    } catch {
      case ex: NoSuchMethodException =>
        // Drop back to Hadoop 1 constructor
        jobClass.getConstructor(classOf[Configuration]).newInstance(config).asInstanceOf[Job]
    }
  }

}

class CartesianFastaFunctions(rdd:RDD[(Region,Nucleotide)]) {
  def filterCartesianFasta = rdd.filter { case (region, nucl) =>
    region.chr == nucl.chr &&
      region.start - 700 <= nucl.position &&
      region.end + 700 >= nucl.position
  }
}

class CartesianSamFunctions(rdd:RDD[(Region,SAMRecord)]) {
  def filterCartesianSam = rdd.filter { case (region, samRec) =>
    region.chr == samRec.getContig &&
      samRec.getStart < region.end &&
      samRec.getEnd > region.start
  }
}

