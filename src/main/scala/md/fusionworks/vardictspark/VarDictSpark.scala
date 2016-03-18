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

  val localArgs = Array(
    "-c", "1", "-S", "2", "-E", "3", "-s", "2", "-e", "3", "-g", "4",
    "-G", "/home/ubuntu/work/data/chr20_dataset/raw/human_b37_20.fasta",
    "-b", "/home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bam",
    "/home/ubuntu/work/data/chr20_dataset/raw/dedupped_20.bed"
  )

  val sc = SparkContextFactory.getSparkContext(Some("local[*]"))

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

  val regions = VarDictSpark.loadRegions(bedPath)

  val fasta = VarDictSpark.loadFasta(fastaPath)
  val cartesianFasta = regions.cartesian(fasta).filter { case (region, nucl) =>
    region.chr == nucl.chr &&
      region.start - 700 <= nucl.position &&
      region.end + 700 >= nucl.position
  }.mapValues(n => n.position -> n.value)

  val samRDD = VarDictSpark.sc.loadSamRecordRDD(bamPath)
  val cartesianSam = regions.cartesian(samRDD).filter { case (region, samRec) =>
    region.chr == samRec.getContig &&
      samRec.getStart < region.end &&
      samRec.getEnd > region.start
  }

  val vdElems = cartesianFasta.cogroup(cartesianSam)


  val vars = computeVars(vdElems, conf)

  vars.saveAsTextFile(tmpPath)

  FileUtil.copyMerge(fs, tmpPath, fs, outputPath, true, hadoopConf, null)
  fs.delete(tmpPath, true)

  implicit def str2Path(path: String): Path = new Path(path)

  def computeVars(rDD: RDD[(Region, (Iterable[(Integer, Character)], Iterable[SAMRecord]))], conf: Broadcast[Configuration]) = {
    val chrs = mapToJavaHashMap(
      samRDD.first()
        .getHeader.getSequenceDictionary.getSequences
        .map(s => s.getSequenceName -> s.getSequenceLength.asInstanceOf[Integer]).toMap
    )

    rDD.flatMap { case (region, (ref, bam)) =>
      val sample = VarDict.getSampleNames(conf.value)._1

      val splice: java.util.Set[String] = new java.util.HashSet[String]
      val vars_ = VarDict.toVars(region.toVDRegion, bam.toList, mapToJavaHashMap(ref.toMap), chrs, sample, splice, 0, conf.value)
      VarDict.vardict(region.toVDRegion, vars_._2, sample, splice, conf.value)
    }
  }

  def mapToJavaHashMap[K, V](map: scala.collection.Map[K, V]): util.HashMap[K, V] = {
    new util.HashMap[K, V](map)
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

  def filterFasta(fasta: RDD[Nucleotide], region: Region) = {
    val start = region.start - 700
    val end = region.end + 700
    fasta.filter(n => n.chr == region.chr && n.position >= start && n.position <= end)
  }
}

case class Region(chr: String, start: Int, end: Int, gene: String /*, iStart: Int, iEnd: Int*/) {

  import com.astrazeneca.vardict.{Region => VDRegion}

  def toVDRegion: VDRegion = {
    new VDRegion(chr, start, end, gene)
  }
}

object Region {
  def fromString(s: String) = {
    val arr = s.split("\t")
    Region(arr(0), arr(1).toInt, arr(2).toInt, arr.lift(3).getOrElse(""))
  }
}

case class VarDictRDDElem(region: Region, ref: util.HashMap[Integer, Character], bam: List[SAMRecord])

case class Nucleotide(chr: String, position: Integer, value: Character)