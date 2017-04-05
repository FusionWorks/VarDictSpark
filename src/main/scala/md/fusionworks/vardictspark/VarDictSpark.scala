package md.fusionworks.vardictspark

import java.util
import java.util.UUID

import com.astrazeneca.vardict.{Configuration, VarDict, Main => VDMain}
import htsjdk.samtools.SAMRecord
import md.fusionworks.vardictspark.Implicits._
import md.fusionworks.vardictspark.spark.SparkContextFactory
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._


/**
  * Created by antonstamov on 3/13/16.
  */


object VarDictSpark extends App {

  System.setProperty("HADOOP_USER_NAME", "hdfs")

  val sc = SparkContextFactory.getSparkContext(/*Some("local[*]")*/)

  val conf = VarDictSpark.sc.broadcast(
    VDMain.getConfigurationFromArgs(args)
  )

  val bedPath = conf.value.getBed
  val bamPath = conf.value.getBam
  val fastaPath = conf.value.getFasta
  val outputPath = conf.value.getOutputVcf
  val tmpPath = s"${outputPath}_tmp-${UUID.randomUUID().toString}"

  val hadoopConf = sc.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)
  fs.delete(new Path(outputPath), true)

  val regions = sc.loadRegions(bedPath)

  val fasta = sc.loadFasta(fastaPath)
  val cartesianFasta = regions.cartesian(fasta)
    .filterCartesianFasta.mapValues(n => n.position -> n.value)

  val samRDD = sc.loadSamRecordRDD(bamPath)
  val cartesianSam = regions.cartesian(samRDD).filterCartesianSam

  val vdElems = cartesianFasta.cogroup(cartesianSam)

  val vars = computeVars(vdElems, conf)

  vars.saveAsTextFile(tmpPath)

  FileUtil.copyMerge(fs, tmpPath, fs, outputPath, true, hadoopConf, null)
  fs.delete(tmpPath, true)

  def computeVars(rDD: RDD[(Region, (Iterable[(Integer, Character)], Iterable[SAMRecord]))], conf: Broadcast[Configuration]) = {
    val chrs = sc.broadcast(
      mapToJavaHashMap(
        samRDD.first()
          .getHeader.getSequenceDictionary.getSequences
          .map(s => s.getSequenceName -> s.getSequenceLength.asInstanceOf[Integer]).toMap
      ))
    val sample = sc.broadcast(VarDict.getSampleNames(conf.value)._1)

    rDD
      .map(t => (t._1, mapToJavaHashMap(t._2._1.toMap), t._2._2.toList))
      .filter(t => !t._2.isEmpty && t._3.nonEmpty)
      .flatMap { case (region, ref, bam) =>

        val region_ = if (region.gene.isEmpty) {
          region.copy(gene = bam.head.getReadName)
        } else region

        val splice: java.util.Set[String] = new java.util.HashSet[String]
        val vars_ = VarDict.toVars(region_.toVDRegion, bam, ref, chrs.value, sample.value, splice, 0, conf.value)
        VarDict.vardict(region_.toVDRegion, vars_._2, sample.value, splice, conf.value)

      }
  }

  implicit def str2Path(path: String): Path = new Path(path)

  def mapToJavaHashMap[K, V](map: scala.collection.Map[K, V]): util.HashMap[K, V] = {
    new util.HashMap[K, V](map)
  }


}

